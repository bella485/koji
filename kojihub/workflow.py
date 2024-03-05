import inspect
import json
import logging
import time

import koji
from koji.context import context
from koji.util import dslice
from . import kojihub
from .scheduler import log_both
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

    tables = ['workflow_queue']
    #joinmap = {
    #    'workflow': 'workflow ON workflow_queue.workflow_id = workflow.id',
    #}
    fieldmap = {
        'id': ['workflow_queue.id', None],
        'workflow_id': ['workflow_queue.workflow_id', None],
        'create_time': ['workflow_queue.create_time', None],
        'create_ts': ["date_part('epoch', workflow_queue.create_time)", None],
        'completion_time': ['workflow_queue.completion_time', None],
        'completion_ts': ["date_part('epoch', workflow_queue.completion_time)", None],
        'completed': ['workflow_queue.completed', None],
        'error': ['workflow_queue.error', None],
    }
    default_fields = ('id', 'workflow_id', 'create_ts', 'completion_ts', 'completed', 'error')


def get_queue():
    # TODO limit?
    query = WorkQueueQuery(clauses=[['completed', 'IS', False]], opts={'order': 'id'})
    return query.execute()


def nudge_queue():
    """Run next queue entry, or attempt to refill queue"""
    if queue_next():
        # if we handled a queue item, we're done
        return True
    update_queue()
    handle_slots()
    return False
    # TODO should we return something more informative?


def queue_next():
    """Run next entry in work queue

    :returns: True if an entry ran, False otherwise
    """
    query = QueryProcessor(tables=['workflow_queue'],
                           columns=['id', 'workflow_id'],
                           clauses=['completed IS FALSE'],
                           opts={'order': 'id', 'limit': 1},
                           lock='skip')
    # note the lock=skip with limit 1. This will give us a row lock on the first unlocked row
    row = query.executeOne()
    if not row:
        # either empty queue or all locked
        return False

    logger.debug('Handling work queue id %(id)s, workflow %(workflow_id)s', row)

    try:
        run_workflow(row['workflow_id'])

    finally:
        # mark it done, even if we errored
        update = UpdateProcessor('workflow_queue', clauses=['id=%(id)s'], values=row)
        update.set(completed=True)
        update.rawset(completion_time='NOW()')
        update.execute()

    logger.debug('Finished handling work queue id %(id)s', row)
    return True


def update_queue():
    check_waits()
    clean_queue()


def clean_queue():
    logger.debug('Cleaning old queue entries')
    lifetime = 3600  # XXX config
    delete = DeleteProcessor(
        table='workflow_queue',
        values={'age': f'{lifetime} seconds'},
        clauses=['completed IS TRUE', "completion_time < NOW() - %(age)s::interval"],
    )
    count = delete.execute()
    if count:
        logger.info('Deleted %i old queue entries', count)


def check_waits():
    """Check our wait data and see if we need to update the queue

    Things we're checking for:
    - workflows with fulfilled waits
    - checking to see if waits are fulfilled
    """
    # TODO -- sort out check frequency
    logger.debug('Checking waits')
    query = WaitsQuery(
        clauses=[
            ['seen', 'IS', False],
        ],
        opts={'order': 'id'}
    )

    # index by workflow
    wf_waits = {}
    for info in query.execute():
        wf_waits.setdefault(info['workflow_id'], []).append(info)

    fulfilled = []
    seen = []
    requeue = []
    for workflow_id in wf_waits:
        # first pass: check fulfillment
        for info in wf_waits[workflow_id]:
            if info['fulfilled']:
                # fulfilled but not seen means fulfillment was noted elsewhere
                # mark it seen so we don't keep checking it
                seen.append(info)
            else:
                # TODO we should avoid calling wait.check quite so often
                cls = waits.get(info['wait_type'])
                wait = cls(info)
                if wait.check():
                    info['fulfilled'] = True
                    fulfilled.append(info)
        waiting = []
        nonbatch = []
        # second pass: decide whether to requeue
        for info in wf_waits[workflow_id]:
            if info['fulfilled']:
                # batch waits won't trigger a requeue unless all other waits are fulfilled
                if not info.get('batch'):
                    nonbatch.append(info)
            else:
                waiting.append(info)
        if not waiting or nonbatch:
            requeue.append(workflow_id)

    for info in fulfilled + seen:
        logger.info('Fulfilled %(wait_type)s wait %(id)s for workflow %(workflow_id)s', info)

    if fulfilled:
        update = UpdateProcessor(
            table='workflow_wait',
            clauses=['id IN %(ids)s'],
            values={'ids': [w['id'] for w in fulfilled]},
        )
        update.set(fulfilled=True)
        update.rawset(fulfill_time='NOW()')
        update.set(seen=True)
        update.execute()

    if seen:
        update = UpdateProcessor(
            table='workflow_wait',
            clauses=['id IN %(ids)s'],
            values={'ids': [w['id'] for w in seen]},
        )
        update.set(seen=True)
        update.execute()

    for workflow_id in requeue:
        logger.info('Re-queueing workflow %s', workflow_id)
        insert = InsertProcessor('workflow_queue', data={'workflow_id': workflow_id})
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
        'frozen': ['workflow.frozen', None],
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


class WorkflowFailure(Exception):
    """Raised to explicitly fail a workflow"""
    pass


def run_workflow(workflow_id, opts=None, strict=False):
    query = WorkflowQuery(clauses=[['id', '=', workflow_id]]).query
    query.lock = True  # we must have a lock on the workflow before attempting to run it
    wf = query.executeOne(strict=True)

    if wf['completed']:
        # shouldn't happen, closing the workflow should delete its queue entries
        logger.error('Ignoring completed %(method)s workflow: %(id)i', wf)
        logger.debug('Data: %r, Opts: %r', wf, opts)
        return
    if wf['frozen']:
        logger.warning('Skipping frozen %(method)s workflow: %(id)i', wf)
        return

    cls = workflows.get(wf['method'])
    handler = cls(wf)

    error = None
    savepoint = Savepoint('pre_workflow')
    try:
        handler.run(opts)

    except WorkflowFailure as err:
        # this is deliberate failure, so handle it that way
        error = str(err)
        handler.fail(msg=error)

    except Exception as err:
        # for unplanned exceptions, we assume the worst
        # rollback and freeze the workflow
        savepoint.rollback()
        error = str(err)
        handle_error(wf, error)
        logger.exception('Error handling workflow')

    if strict and error is not None:
        raise koji.GenericError(f'Error handling workflow: {error}')


def run_subtask_step(workflow_id, step):
    opts = {'from_subtask': True, 'step': step}
    run_workflow(workflow_id, opts, strict=True)


def handle_error(info, error):
    # freeze the workflow
    update = UpdateProcessor('workflow', clauses=['id=%(id)s'], values=info)
    update.set(frozen=True)
    update.rawset(update_time='NOW()')
    update.execute()

    # record the error
    error_data = {
        'error': error,  # TODO traceback?
        'workflow_data': info['data'],
    }
    data = {
        'workflow_id': info['id'],
        'data': json.dumps(error_data),
    }
    insert = InsertProcessor('workflow_error', data=data)
    insert.execute()

    # delist the workflow
    for table in ('workflow_wait', 'workflow_slots', 'workflow_queue'):
        delete = DeleteProcessor(table, clauses=['workflow_id = %(id)s'],
                                 values=info)
        delete.execute()


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

    def run(self, opts=None):
        if self.data is None:
            self.setup()
        if opts is None:
            opts = {}

        self.handle_waits()

        # TODO error handling
        step = self.data['steps'].pop(0)
        handler = self.get_handler(step)
        if 'step' in opts and opts['step'] != step:
            raise koji.GenericError(f'Step mismatch {opts["step"]} != {step}')

        is_subtask = getattr(handler, 'subtask', False)
        if opts.get('from_subtask'):
            # we've been called via a workflowStep task
            if not is_subtask:
                raise koji.GenericError(f'Not a subtask step: {step}')
            # otherwise we're good
        elif is_subtask:
            # this step needs to run via a subtask
            self.task('workflowStep', {'workflow_id': self.info['id'], 'step': step})
            # TODO handle task failure without looping
            return

        # TODO slots are a better idea for tasks than for workflows
        slot = getattr(handler, 'slot', None)
        if slot:
            # a note about timing. We don't request a slot until we're otherwise ready to run
            # We don't want to hold a slot if we're waiting on something else.
            if not get_slot(slot, self.info['id']):
                self.wait_slot(slot, request=False)  # get_slot made the request for us
                return
            logger.debug('We have slot %s. Proceeding.', slot)

        # auto-fill handler params
        kwargs = {}
        params = inspect.signature(handler).parameters
        for key in params:
            param = params[key]
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                # step handlers shouldn't use these, but we'll be nice
                logger.warning('Ignoring variable args for %s', handler)
                continue
            if key == 'workflow':
                kwargs[key] = self
            elif key in self.params:
                kwargs[key] = self.params[key]
            elif key in self.data:
                kwargs[key] = self.data[key]

        self.log(f'Running workflow step {step}')
        logger.debug('Step args: %r', kwargs)
        handler(**kwargs)

        if slot:
            # we only hold the slot during the execution of the step
            free_slot(slot, self.info['id'])

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

    def handle_waits(self):
        query = WaitsQuery(
            clauses=[['workflow_id', '=', self.info['id']], ['handled', 'IS', False]],
            opts={'order': 'id'})
        mywaits = query.execute()
        waiting = []
        for info in mywaits:
            if not info['fulfilled']:
                # TODO should we call check here as well?
                waiting.append(info)
            else:
                cls = waits.get(info['wait_type'])
                wait = cls(info)
                wait.handle(workflow=self)
        return bool(waiting)

    def log(self, msg, level=logging.INFO):
        log_both(msg, task_id=self.info['stub_id'], level=level)

    def setup(self):
        """Called to set up the workflow run"""
        logger.debug('Setting up workflow: %r', self.info)
        self.data = {'steps': self.get_steps()}
        # also open our stub task
        stub = kojihub.Task(self.info['stub_id'])
        stub.open(workflow=True)

    @classmethod
    def step(cls, name=None):
        """Decorator to add steps outside of class"""
        # note this can't be used IN the class definition
        steps = getattr(cls, 'STEPS', None)
        if steps is None:
            steps = cls.STEPS = []
        handlers = getattr(cls, '_step_handlers', None)
        if handlers is None:
            handlers = cls._step_handlers = {}

        def decorator(func):
            nonlocal name
            # also updates nonlocal steps
            if name is None:
                name = func.__name__
            steps.append(name)
            handlers[name] = func
            return func

        return decorator

    def get_handler(self, step):
        handlers = getattr(self, '_step_handlers', {})
        if handlers and step in handlers:
            return handlers[step]
        else:
            return getattr(self, step)

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
        self.log(f'Waiting for {wait_type}: {params}')
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

    def wait_slot(self, name, request=True):
        self.wait('slot', {'name': name})
        if request:
            request_slot(name, self.info['id'])

    def start(self):
        raise NotImplementedError('start method not defined')

    def close(self, result='complete', stub_state='CLOSED'):
        # TODO - the result field needs to be handled better
        self.log('Closing %(method)s workflow' % self.info)

        for table in ('workflow_wait', 'workflow_slots', 'workflow_queue'):
            delete = DeleteProcessor(table, clauses=['workflow_id = %(id)s'],
                                     values=self.info)
            delete.execute()

        update = UpdateProcessor('workflow', clauses=['id=%(id)s'], values=self.info)
        update.set(data=json.dumps(self.data))
        update.rawset(update_time='NOW()')
        update.set(completed=True)
        update.set(result=result)
        update.execute()

        # also close our stub task
        # we don't worry about checks here because the entry is just a stub
        logger.info('Closing workflow task %(stub_id)i', self.info)
        stub = kojihub.Task(self.info['stub_id'])
        stub._close(result, koji.TASK_STATES[stub_state], encode=True)
        # TODO handle failure

    def cancel(self):
        # TODO we need to do more here, but for now
        self.close(result='canceled', stub_state='CANCELED')

    def fail(self, msg=None):
        # TODO we need to do more here, but for now
        if msg is not None:
            msg = f'Workflow failed - {msg}'
        else:
            msg = 'Workflow failed'
        self.close(result=msg, stub_state='FAILED')

    def requeue(self):
        self.log('Queuing %(method)s workflow' % self.info)
        insert = InsertProcessor('workflow_queue', data={'workflow_id': self.info['id']})
        insert.execute()

    def update(self):
        update = UpdateProcessor('workflow', clauses=['id=%(id)s'], values=self.info)
        update.set(data=json.dumps(self.data))
        update.rawset(update_time='NOW()')
        update.execute()


def subtask():
    # TODO args?
    """Decorator to indicate that a step handler should run via a subtask"""
    def decorator(handler):
        handler.subtask = True
        return handler

    return decorator


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
    log_both(f'Adding {method} workflow', task_id=stub_id)

    if queue:
        # also add it to the work queue so it will start
        insert = InsertProcessor('workflow_queue', data={'workflow_id': data['id']})
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
        'seen': ['workflow_wait.seen', None],
        'handled': ['workflow_wait.handled', None],
    }


class BaseWait:

    def __init__(self, info):
        self.info = info
        self.params = info['params']

    def check(self):
        raise NotImplementedError('wait check not defined')

    def set_handled(self):
        # TODO what should we do if not fulfilled yet?
        update = UpdateProcessor('workflow_wait', clauses=['id = %(id)s'], values=self.info)
        update.set(handled=True)
        update.execute()

    def handle(self):
        self.set_handled()


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

    def handle(self, workflow):
        self.set_handled()
        task = kojihub.Task(self.info['params']['task_id'])
        tinfo = task.getInfo()
        ret = {'task': tinfo}
        if tinfo['state'] == koji.TASK_STATES['FAILED']:
            if not self.info['params'].get('canfail', False):
                raise koji.GenericError(f'Workflow task {tinfo["id"]} failed')
                # TODO workflow failure
            # otherwise we keep going
        elif tinfo['state'] == koji.TASK_STATES['CANCELED']:
            # TODO unclear if canfail applies here
            raise koji.GenericError(f'Workflow task {tinfo["id"]} canceled')
        elif tinfo['state'] == koji.TASK_STATES['CLOSED']:
            # shouldn't be a fault
            ret['result'] = task.getResult()
        else:
            # should not happen
            raise koji.GenericError(f'Task not completed: {tinfo}')
        # TODO: update workflow data?
        return ret

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
        # handle_slots will mark us fulfilled, so no point in further checking here
        return False


def slot(name):
    """Decorator to indicate that a step handler requires a slot"""
    def decorator(handler):
        handler.slot = name
        return handler

    return decorator


def request_slot(name, workflow_id):
    logger.info('Requesting %s slot for workflow %i', name, workflow_id)
    data = {
        'name': name,
        'workflow_id': workflow_id,
    }
    upsert = UpsertProcessor(table='workflow_slots', data=data, skip_dup=True)
    upsert.execute()
    # table has: UNIQUE (name, workflow_id)
    # so this is a no-op if we already have a request, or are already holding the slot


def free_slot(name, workflow_id):
    logger.info('Freeing %s slot for workflow %i', name, workflow_id)
    values = {
        'name': name,
        'workflow_id': workflow_id,
    }
    delete = DeleteProcessor(
            table='workflow_slots',
            clauses=['name = %(name)s', 'workflow_id = %(workflow_id)s'],
            values=values)
    delete.execute()


def get_slot(name, workflow_id):
    """Check for and/or attempt to acquire slot

    :returns: True if slot is held, False otherwise

    If False, then the slot is *requested*
    """
    values = {
        'name': name,
        'workflow_id': workflow_id,
    }
    query = QueryProcessor(
        tables=['workflow_slots'],
        columns=['id', 'held'],
        clauses=['name = %(name)s', 'workflow_id = %(workflow_id)s'],
        values=values,
    )
    slot = query.executeOne()
    if not slot:
        request_slot(name, workflow_id)
    elif slot['held']:
        return True

    handle_slots()  # XXX?

    # check again
    slot = query.executeOne()
    return slot and slot['held']


def handle_slots():
    """Check slot requests and see if we can grant them"""

    if not db_lock('workflow_slots', wait=False):
        return

    logger.debug('Checking slots')

    query = QueryProcessor(
        tables=['workflow_slots'],
        columns=['id', 'name', 'workflow_id', 'held'],
        opts={'order': 'id'},
    )

    # index needed and held by name
    need_idx = {}
    held_idx = {}
    slots = query.execute()
    for slot in slots:
        if slot['held']:
            held_idx.setdefault(slot['name'], []).append(slot)
        else:
            need_idx.setdefault(slot['name'], []).append(slot)

    grants = []
    for name in need_idx:
        need = need_idx[name]
        held = held_idx.get(name, [])
        limit = 3  # XXX CONFIG
        logger.debug('Slot %s: need %i, held %i', name, len(need), len(held))
        while need and len(held) < limit:
            slot = need.pop(0)  # first come, first served
            held.append(slot)
            grants.append(slot)

    # update the slots
    if grants:
        update = UpdateProcessor(table='workflow_slots',
                                 clauses=['id IN %(ids)s'],
                                 values={'ids': [s['id'] for s in grants]})
        update.set(held=True)
        update.rawset(grant_time='NOW()')
        update.execute()

    # also mark any waits fulfilled
    for slot in grants:
        update = UpdateProcessor(
            'workflow_wait',
            clauses=[
                "wait_type = 'slot'",
                'fulfilled IS FALSE',
                'workflow_id = %(workflow_id)s',
                "(params->>'name') = %(name)s",  # note the ->>
            ],
            values=slot)
        update.set(fulfilled=True)
        update.execute()


@workflows.add('test')
class TestWorkflow(BaseWorkflow):

    # XXX remove this test code

    # STEPS = ['start', 'finish']
    PARAMS = {'a': int, 'b': (int, type(None)), 'c': str}


@TestWorkflow.step()
def start(workflow, a, b):
    # fire off a do-nothing task
    logger.info('TEST WORKFLOW START')
    workflow.data['task_id'] = workflow.task('sleep', {'n': 1})


@subtask()
@TestWorkflow.step()
def finish():
    time.sleep(10)
    logger.info('TEST WORKFLOW FINISH')


@workflows.add('new-repo')
class NewRepoWorkflow(BaseWorkflow):

    STEPS = ['repo_init', 'repos', 'repo_done']
    PARAMS = {
        'tag': (int, str, dict),
        'event': (int, type(None)),
        'opts': (dict,),
    }

    @subtask()
    def repo_init(self, tag, event=None, opts=None):
        tinfo = kojihub.get_tag(tag, strict=True, event=event)
        event = kojihub.convert_value(event, cast=int, none_allowed=True)
        if opts is None:
            opts = {}
        opts = dslice(opts, ('with_src', 'with_debuginfo', 'with_separate_src'), strict=False)
        # TODO further opts validation?
        repo_id, event_id = kojihub.repo_init(tinfo['id'], event=event,
                                              task_id=self.info['stub_id'], **opts)
        repo_info = kojihub.repo_info(repo_id)
        del repo_info['creation_time']  # json unfriendly
        kw = {'tag': tinfo, 'repo': repo_info, 'opts': opts}
        self.data['prep_id'] = self.task('prepRepo', kw)
        self.data['repo'] = repo_info

    def repos(self, prep_id, repo):
        # TODO better mechanism for fetching task result
        prepdata = kojihub.Task(prep_id).getResult()
        repo_tasks = []
        for arch in prepdata['needed']:
            params = {'repo_id': repo['id'], 'arch': arch, 'oldrepo': prepdata['oldrepo']}
            repo_tasks[arch] = self.task('createrepo', params)
            # TODO fail workflow on any failed subtask
        self.data['cloned'] = prepdata['cloned']
        self.data['repo_tasks'] = repo_tasks

    @subtask()
    def repo_done(self, repo, cloned, repo_tasks, event=None):
        data = cloned.copy()
        for arch in repo_tasks:
            data[arch] = kojihub.Task(repo_tasks[arch]).getResult()

        kwargs = {}
        if event is not None:
            kwargs['expire'] = True
        if cloned:
            kwargs['repo_json_updates'] = {
                'cloned_from_repo_id': 0,   # XXX
                'cloned_archs': list(sorted(cloned)),
            }
        kojihub.repo_done(repo['id'], data, **kwargs)

        # do we need a return?
        return repo['id'], repo['event_id']


class WorkflowExports:
    # TODO: would be nice to mimic our registry approach in kojixmlrpc
    # XXX most of these need access controls
    getQueue = staticmethod(get_queue)
    nudge = staticmethod(nudge_queue)
    updateQueue = staticmethod(update_queue)
    add = staticmethod(add_workflow)
    cancel = staticmethod(cancel_workflow)
