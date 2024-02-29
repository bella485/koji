import json
import logging
import time

import koji
from koji.context import context
from . import kojihub
from .db import QueryProcessor, InsertProcessor, UpsertProcessor, UpdateProcessor, \
    DeleteProcessor, QueryView, db_lock, nextval, Savepoint


logger = logging.getLogger('koji.workflow')


"""

DRAFT

Playing around with the idea of hub workflows
Starting with basic tasks, e.g.

* build task
* newRepo

"""


class WorkQueueQuery(QueryView):

    tables = ['work_queue']
    #joinmap = {
    #    'workflow': 'workflow ON work_queue.workflow_id = workflow.id',
    #}
    fieldmap = {
        'id': ['work_queue.id', None],
        'workflow_id': ['work_queue.workflow_id', None],
        'create_time': ['work_queue.create_time', None],
        'create_ts': ["date_part('epoch', work_queue.create_time)", None],
        'completion_time': ['work_queue.completion_time', None],
        'completion_ts': ["date_part('epoch', work_queue.completion_time)", None],
        'completed': ['work_queue.completed', None],
        'error': ['work_queue.error', None],
    }
    default_fields = ('id', 'workflow_id', 'create_ts', 'completion_ts', 'completed', 'error')


def get_queue():
    # TODO limit?
    query = WorkQueueQuery(clauses=[['completed', 'IS', False]], opts={'order': 'id'})
    return query.execute()


def run_queue(queue_id):
    # run a work queue entry
    queue_id = kojihub.convert_value(queue_id, cast=int)

    logger.debug('Handling work queue id %s', queue_id)
    # TODO maybe use scheduler logging mechanism?
    query = WorkQueueQuery(clauses=[['id', '=', queue_id]], opts={'rowlock': True})
    # XXX fix how rowlock is handled
    job = query.executeOne()
    if not job:
        raise koji.GenericError('Invalid queue id: %s', queue_id)
        # TODO should we be less strict?
    if job['completed']:
        raise koji.GenericError('Queue entry already completed: %s', queue_id)

    # otherwise, go ahead
    handle_job(job)

    # and mark it done
    update = UpdateProcessor('work_queue', clauses=['id=%(id)s'], values=job)
    update.set(completed=True)
    update.rawset(completion_time='NOW()')
    update.execute()

    logger.debug('Finished handling work queue id %s', queue_id)


def update_queue():
    handle_waits()
    clean_queue()


def clean_queue():
    logger.debug('Cleaning old queue entries')
    lifetime = 3600  # XXX config
    delete = DeleteProcessor(
        table='work_queue',
        values={'age': f'{lifetime} seconds'},
        clauses=['completed IS TRUE', "completion_time < NOW() - %(age)s::interval"],
    )
    count = delete.execute()
    if count:
        logger.info('Deleted %i old queue entries', count)


def handle_waits():
    """Check our wait data and see if we need to update the queue

    Things we're checking for:
    - workflows with fulfilled waits
    - checking to see if waits are fulfilled
    """
    # TODO -- sort out check frequency
    logger.debug('Checking waits')
    query = WaitsQuery(
        clauses=[
            ['handled', 'IS', False],
        ]
    )

    # index by workflow
    wf_waits = {}
    for info in query.execute():
        wf_waits.setdefault(info['workflow_id'], []).append(info)

    fulfilled = []
    handled = []
    requeue = []
    for workflow_id in wf_waits:
        waiting = []
        for info in wf_waits[workflow_id]:
            if info['fulfilled']:
                handled.append(info)
            else:
                # TODO we should avoid calling wait.check quite so often
                cls = waits.get(info['wait_type'])
                wait = cls(info)
                if wait.check():
                    fulfilled.append(info)
                else:
                    waiting.append(info)
        if not waiting:
            requeue.append(workflow_id)

    for info in fulfilled:
        logger.info('Fulfilled %(wait_type)s wait %(id)s for workflow %(workflow_id)s', info)
    if fulfilled:
        # we can do these in single update
        update = UpdateProcessor(
            table='workflow_wait',
            clauses=['id IN %(ids)s'],
            values={'ids': [w['id'] for w in fulfilled]},
        )
        update.set(fulfilled=True)
        update.set(handled=True)
        update.execute()

    for info in handled:
        logger.info('Handled %(wait_type)s wait %(id)s for workflow %(workflow_id)s', info)
    if handled:
        # we can do these in single update
        update = UpdateProcessor(
            table='workflow_wait',
            clauses=['id IN %(ids)s'],
            values={'ids': [w['id'] for w in handled]},
        )
        update.set(handled=True)
        update.execute()

    for workflow_id in requeue:
        logger.info('Re-queueing workflow %s', workflow_id)
        insert = InsertProcessor('work_queue', data={'workflow_id': workflow_id})
        insert.execute()


class WorkflowQuery(QueryView):

    tables = ['workflow']
    joinmap = {
        'users': 'users ON users.id = workflow.owner',
    }
    fieldmap = {
        'id': ['workflow.id', None],
        'stub_id': ['stub_id', None],
        'started': ['workflow.started', None],
        'completed': ['workflow.completed', None],
        'create_time': ['workflow.create_time', None],
        'start_time': ['workflow.start_time', None],
        'update_time': ['workflow.update_time', None],
        'create_ts': ["date_part('epoch', workflow.create_time)", None],
        'start_ts': ["date_part('epoch', workflow.start_time)", None],
        'update_ts': ["date_part('epoch', workflow.update_time)", None],
        'owner': ['workflow.owner', None],
        'owner_name': ['users.name', 'users'],
        'method': ['workflow.method', None],
        'params': ['workflow.params', None],
        'result': ['workflow.result', None],
        'data': ['workflow.data', None],
    }


def handle_job(job):
    wf = WorkflowQuery(clauses=[['id', '=', job['workflow_id']]]).executeOne(strict=True)
    if wf['completed']:
        logger.error('Ignoring completed %(method)s workflow in queue: %(id)i', wf)
        logger.debug('Data: %r', wf)
        return
    logger.debug('Handling workflow: %r', wf)
    cls = workflows.get(wf['method'])
    handler = cls(wf)
    try:
        handler.run()
    except Exception as err:
        handle_error(job, err)
        raise  # XXX


def handle_error(job, err):
    # for now we mark it completed but include the error
    # TODO retries?
    # XXX what do we do about the workflow?
    update = UpdateProcessor('work_queue', clauses=['id=%(id)s'], values=job)
    update.set(completed=True)
    update.set(error=str(err))
    update.rawset(completion_time='NOW()')
    update.execute()


class SimpleRegistry:

    def __init__(self):
        self.handlers = {}

    def add(self, name):
        # used as a decorator
        def func(handler):
            self.handlers[name] = handler
            # don't error on duplicates in case a plugin needs to override
            return handler
        return func

    def get(self, name, strict=True):
        if strict:
            return self.handlers[name]
        else:
            return self.handlers.get(name)


workflows = SimpleRegistry()


class BaseWorkflow:

    def __init__(self, info):
        self.info = info
        self.params = info['params']
        self.data = info['data']
        self.waiting = False

    def run(self):
        if self.data is None:
            self.setup()

        # TODO error handling
        step = self.data['steps'].pop(0)

        logger.debug('Running %s step for workflow %s', step, self.info['id'])
        func = getattr(self, step)
        func()

        # are we done?
        if not self.data['steps']:
            logger.debug('No more steps in workflow')
            self.close()
            return

        # re-queue ourselves if we're not waiting
        if not self.waiting:
            self.requeue()

        # update the db
        self.update()

    def setup(self):
        """Called to set up the workflow run"""
        logger.debug('Setting up workflow: %r', self.info)
        self.data = {'steps': self.get_steps()}
        # also open our stub task
        # we don't worry about checks here because the entry is just a stub
        update = UpdateProcessor('task', clauses=['id = %(stub_id)s'], values=self.info)
        update.set(state=koji.TASK_STATES['OPEN'])
        update.execute()

    def get_steps(self):
        """Get the initial list of steps

        Classes can define STEPS, or provide a start method.
        The steps queue can be also be modified at runtime, e.g. by set_next()
        """
        steps = getattr(self, 'STEPS', None)
        if not steps:
            steps = ['start']
        else:
            steps = list(steps)  # copy
        return steps

    @classmethod
    def get_param_spec(cls):
        """Get the rules about params"""
        spec = getattr(cls, 'PARAMS', None)
        if isinstance(spec, (list, tuple, set)):
            spec = {k: None for k in spec}
        return spec

    @classmethod
    def check_params(cls, params):
        # TODO conversion mechanism
        if not isinstance(params, dict):
            raise koji.ParameterError('Workflow parameters must be given as a dictionary')
        spec = cls.get_param_spec()
        if spec is None:
            return
        for key in params:
            if key not in spec:
                raise koji.ParameterError(f'Invalid parameter name: {key}')
        for key in spec:
            pspec = spec[key]
            if pspec is None or pspec is Ellipsis:
                continue
            if isinstance(pspec, dict):
                pspec = ParamSpec(**pspec)
            elif not isinstance(pspec, ParamSpec):
                pspec = ParamSpec(pspec)
            if key in params:
                if not pspec.check(params[key]):
                    raise koji.ParameterError(f'Invalid type for parameter {key}: '
                                              f'expected {str(pspec)}')
            if pspec.required and key not in params:
                raise koji.ParameterError(f'Missing required parameter: {key}')

    def set_next(self, step):
        self.data['steps'].insert(0, step)

    def wait_task(self, task_id):
        self.wait('task', {'task_id': task_id})

    def wait(self, wait_type, params):  # TODO maybe **params?
        data = {
            'workflow_id': self.info['id'],
            'wait_type': wait_type,
            'params': json.dumps(params),
        }
        insert = InsertProcessor('workflow_wait', data=data)
        insert.execute()
        self.waiting = True

    def task(self, method, params, opts=None, wait=True):
        if opts is None:
            opts = {}
        # TODO limit opts?
        opts['parent'] = self.info['stub_id']
        opts['workflow_id'] = self.info['id']
        # we only pass by name
        args = koji.encode_args(**params)
        task_id = kojihub.make_task(method, args, **opts)
        if wait:
            self.wait_task(task_id)

    def start(self):
        raise NotImplementedError('start method not defined')

    def close(self, result='complete', stub_state='CLOSED'):
        # TODO - the result field needs to be handled better
        logger.info('Closing %(method)s workflow %(id)i', self.info)
        # we shouldn't have any waits but...
        delete = DeleteProcessor('workflow_wait', clauses=['workflow_id = %(id)s'],
                                 values=self.info)
        n = delete.execute()
        if n:
            logger.error('Dangling waits for %(method)s workflow %(id)i', self.info)

        update = UpdateProcessor('workflow', clauses=['id=%(id)s'], values=self.info)
        update.set(data=json.dumps(self.data))
        update.rawset(update_time='NOW()')
        update.set(completed=True)
        update.set(result=result)
        update.execute()

        # also close our stub task
        # we don't worry about checks here because the entry is just a stub
        logger.info('Closing workflow task %(stub_id)i', self.info)
        # we shouldn't have any waits but...
        update = UpdateProcessor('task', clauses=['id = %(stub_id)s'], values=self.info)
        update.set(state=koji.TASK_STATES[stub_state])
        # TODO handle failure
        update.execute()

    def cancel(self):
        # TODO we need to do more here, but for now
        self.close(result='canceled', stub_state='CANCELED')

    def requeue(self):
        insert = InsertProcessor('work_queue', data={'workflow_id': self.info['id']})
        insert.execute()

    def update(self):
        update = UpdateProcessor('workflow', clauses=['id=%(id)s'], values=self.info)
        update.set(data=json.dumps(self.data))
        update.rawset(update_time='NOW()')
        update.execute()


class ParamSpec:

    def __init__(self, rule, required=False):
        self.required = False
        self.rule = rule

    def check(self, value):
        # for now, just assume a couple simple options
        if isinstance(self.rule, tuple):
            # set of allowed types
            return isinstance(value, self.rule)
        elif callable(self.rule):
            try:
                self.rule(value)
                return True
            except (TypeError, ValueError, koji.ParameterError):
                return False

    def __str__(self):
        # used in error messages
        if isinstance(self.rule, tuple):
            return ', '.join(sorted([str(t.__name__) for t in self.rule]))
        elif callable(self.rule):
            return self.rule.__name__
        else:
            return 'unknown'


def add_workflow(method, params, queue=True):
    context.session.assertLogin()
    # TODO adjust access check?
    method = kojihub.convert_value(method, cast=str)
    cls = workflows.get(method, strict=False)
    if not cls:
        raise koji.GenericError(f'Unknown workflow method: {method}')
    cls.check_params(params)
    queue = kojihub.convert_value(queue, cast=bool)

    # Make our stub task entry
    workflow_id = nextval('workflow_id_seq')
    args = koji.encode_args(method, params, workflow_id=workflow_id)
    stub_id = kojihub.make_task('workflow', args, workflow=True)

    # TODO more validation?
    # TODO policy hook
    # TODO callbacks
    data = {
        'id': workflow_id,
        'stub_id': stub_id,
        'owner': context.session.user_id,
        'method': method,
        'params': json.dumps(params),
    }
    insert = InsertProcessor('workflow', data=data)
    insert.execute()

    if queue:
        # also add it to the work queue so it will start
        insert = InsertProcessor('work_queue', data={'workflow_id': data['id']})
        insert.execute()

    # TODO return full info?
    return data['id']


def cancel_workflow(workflow_id):
    context.session.assertLogin()
    workflow_id = kojihub.convert_value(workflow_id, cast=int)
    wf = WorkflowQuery(clauses=[['id', '=', workflow_id]]).executeOne(strict=True)
    if context.session.user_id != wf['owner']:
        # TODO better access check
        context.session.assertPerm('admin')
    if wf['completed']:
        raise koji.GenericError('Workflow is already completed')
    cls = workflows.get(wf['method'])
    handler = cls(wf)
    handler.cancel()


waits = SimpleRegistry()


class WaitsQuery(QueryView):

    tables = ['workflow_wait']
    fieldmap = {
        'id': ['workflow_wait.id', None],
        'workflow_id': ['workflow_wait.workflow_id', None],
        'wait_type': ['workflow_wait.wait_type', None],
        'params': ['workflow_wait.params', None],
        'create_time': ['workflow_wait.create_time', None],
        'create_ts': ["date_part('epoch', workflow_wait.create_time)", None],
        'fulfilled': ['workflow_wait.fulfilled', None],
        'handled': ['workflow_wait.handled', None],
    }


class BaseWait:

    def __init__(self, info):
        self.info = info
        self.params = info['params']

    def check(self):
        raise NotImplementedError('wait check not defined')

    # XXX does it make sense to update state here?
    def set_fulfilled(self):
        update = UpdateProcessor('workflow_wait', clauses=['id = %(id)s'], values=self.info)
        update.set(fulfilled=True)
        update.execute()

    # XXX does it make sense to update state here?
    def set_handled(self):
        # TODO what should we do if not fulfilled yet?
        update = UpdateProcessor('workflow_wait', clauses=['id = %(id)s'], values=self.info)
        update.set(handled=True)
        update.execute()


@waits.add('task')
class TaskWait(BaseWait):

    END_STATES = {koji.TASK_STATES[s] for s in ('CLOSED', 'CANCELED', 'FAILED')}

    def check(self):
        # we also have triggers to update these, but this is a fallback
        params = self.info['params']
        query = QueryProcessor(tables=['task'], columns=['state'],
                               clauses=['id = %(task_id)s'], values=params)
        state = query.singleValue()
        return (state in self.END_STATES)

    @staticmethod
    def task_done(task_id):
        # TODO catch errors?
        update = UpdateProcessor(
            'workflow_wait',
            clauses=[
                "wait_type = 'task'",
                'fulfilled IS FALSE',
                "(params->'task_id')::int = %(task_id)s",
                # int cast required because -> returns jsonb
            ],
            values={'task_id': task_id})
        update.set(fulfilled=True)
        update.execute()


@waits.add('slot')
class SlotWait(BaseWait):

    def check(self):
        # we also have triggers to update these, but this is a fallback
        params = self.info['params']
        query = QueryProcessor(
            tables=['workflow_slots'],
            columns=['id'],
            clauses=['name = %(name)s', 'workflow_id = %(workflow_id)s'],
            values={'name': params['name'], 'workflow_id': self.info['workflow_id']},
        )
        slot_id = query.singleValue()
        return (slot_id is not None)


def handle_slots():
    """Check slot waits and see if we can fulfill them"""

    query = WaitsQuery(
        clauses=[
            ['fulfilled', 'IS', False],
            ['wait_type', '=', 'slot'],
        ],
        opts={'order': 'id'},  # oldest first
    )

    by_name = {}
    for wait in query.execute():
        name = wait['params']['name']
        by_name.setdefault(name, []).append(wait)

    for name in sorted(by_name):
        query = QueryProcessor(
                    tables=['workflow_slots'],
                    columns=['id', 'num'],
                    clauses=['name = %(name)s'],
                    values={'name': name},
        )
        limit = 10  # XXX CONFIG
        held = query.execute()
        if len(held) >= limit:
            # all in use
            continue
        waits = by_name[name]
        held = set(held)
        for num in range(limit):
            if num in held:
                continue
            if not waits:
                break
            # try to take it
            wait = waits[0]
            data = {
                'name': name,
                'workflow_id': wait['workflow_id'],
                'num': num,
            }
            insert = InsertProcessor(table='workflow_slots', data=data)
            savepoint = Savepoint('pre_slot_insert')
            try:
                insert.execute()
            except Exception:
                # there must be a parallel call
                savepoint.rollback()
                logger.debug('Failed to acquire workflow slot')
                # XXX how do we avoid duplicate fulfillments by parallel instances?
                continue
            # success! pop this wait so next pass can handle the next
            waits.pop(0)


@workflows.add('test')
class TestWorkflow(BaseWorkflow):

    # XXX remove this test code

    STEPS = ['start', 'finish']
    PARAMS = {'a': int, 'b': (int, type(None)), 'c': str}

    def start(self):
        # fire off a do-nothing task
        logger.info('TEST WORKFLOW START')
        task_id = self.task('sleep', {'n': 1})

    def finish(self):
        # XXX how do we propagate task_id?
        logger.info('TEST WORKFLOW FINISH')


@workflows.add('new-repo')
class NewRepoWorkflow(BaseWorkflow):

    STEPS = ['start', 'repos', 'finalize']

    def start(self):
        # TODO validate params
        kw = self.params
        # ??? should we call repo_init ourselves?
        task_id = self.task('initRepo', kw)
        # TODO mechanism for task_id value to persist to next step

    def repos(self):
        # TODO fetch archlist from task
        repo_tasks = []
        for arch in self.needed_arches:
            params = {'repo_id': repo_id, 'arch': arch, 'oldrepo': oldrepo}
            repo_tasks[arch] = self.task('createrepo', params)

    def finalize(self):
        # TODO fetch params from self/tasks
        repo_done(...)


class WorkflowExports:
    # TODO: would be nice to mimic our registry approach in kojixmlrpc
    #handleWorkQueue = staticmethod(handle_work_queue)
    getQueue = staticmethod(get_queue)
    runQueue = staticmethod(run_queue)
    updateQueue = staticmethod(update_queue)
    add = staticmethod(add_workflow)
    cancel = staticmethod(cancel_workflow)
