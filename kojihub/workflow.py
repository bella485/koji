import json
import logging
import time

import koji
from koji.context import context
from . import kojihub
from .db import QueryProcessor, InsertProcessor, UpsertProcessor, UpdateProcessor, \
    DeleteProcessor, QueryView, db_lock, nextval


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


def handle_work_queue(force=False):
    # This is called regularly by kojira to keep the work flowing
    if not db_lock('work_queue', wait=force):
        # already running elsewhere
        return {}

    logger.debug('Handling work queue')
    # TODO maybe move this to scheduler and use that logging mechanism
    start = time.time()

    # first come, first served
    maxjobs = 10  # XXX config
    maxtime = 30  # XXX config
    query = WorkQueueQuery(clauses=[['completed', 'IS', False]], opts={'order':'id'})
    n = 0
    for n, job in enumerate(query.iterate(), start=1):
        # TODO transaction isolation
        handle_job(job)
        update = UpdateProcessor('work_queue', clauses=['id=%(id)s'], values=job)
        update.set(completed=True)
        update.rawset(completion_time='NOW()')
        update.execute()
        if n >= maxjobs:
            break
        if time.time() - start >= maxtime:
            break
    if n:
        logger.debug('Handled %i jobs', n)

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
        return func

    def get(self, name):
        return self.handlers[name]


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
        steps = getattr(self, 'STEPS')
        if not steps:
            steps = ['start']
        return steps

    def set_next(self, step):
        self.data['steps'].insert(0, step)

    def wait_task(self, task_id):
        self.wait('task', {'task_id': task_id})

    def wait(self, wait_type, params):  # TODO maybe **params?
        data = {
            'workflow_id': self.info['id'],
            'wait_type': 'task',
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

    def close(self, result='complete'):
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
        if result == 'canceled':
            # XXX this is a dumb check
            update.set(state=koji.TASK_STATES['CANCELED'])
        else:
            update.set(state=koji.TASK_STATES['CLOSED'])
        # TODO handle failure
        update.execute()

    def cancel(self):
        # TODO we need to do more here, but for now
        self.close(result='canceled')

    def requeue(self):
        insert = InsertProcessor('work_queue', data={'workflow_id': self.info['id']})
        insert.execute()

    def update(self):
        update = UpdateProcessor('workflow', clauses=['id=%(id)s'], values=self.info)
        update.set(data=json.dumps(self.data))
        update.rawset(update_time='NOW()')
        update.execute()


def add_workflow(method, params, queue=True):
    context.session.assertLogin()
    # TODO adjust access check?
    method = kojihub.convert_value(method, cast=str)
    if not workflows.get(method):
        raise koji.GenericError(f'Unknown workflow method: {method}')
    params = kojihub.convert_value(params, cast=dict)
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


@workflows.add('test')
class TestWorkflow(BaseWorkflow):

    STEPS = ['start', 'finish',]

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
    handleWorkQueue = staticmethod(handle_work_queue)
    add = staticmethod(add_workflow)
    cancel = staticmethod(cancel_workflow)
