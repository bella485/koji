import json
import logging
import time

import koji
from koji.context import context
from . import kojihub
from .db import QueryProcessor, InsertProcessor, UpsertProcessor, UpdateProcessor, \
    DeleteProcessor, QueryView, db_lock, nextval


logger = logging.getLogger('koji.scheduler')


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

    # TODO maybe move this to scheduler and use that logging mechanism
    start = time.time()

    # first come, first served
    maxjobs = 10  # XXX config
    maxtime = 30  # XXX config
    query = WorkQueueQuery(clauses=[['completed', 'IS', False]], opts={'order':'id'})
    for n, job in enumerate(query.iterate()):
        # TODO transaction isolation
        handle_job(job)
        if n >= maxjobs:
            break
        if time.time() - start >= maxtime:
            break

    # clean up old entries
    lifetime = 3600  # XXX config
    delete = DeleteProcessor(
            table='work_queue',
            values={'age': f'{lifetime} seconds'},
            clauses=['completed IS TRUE', "completion_time < NOW() - %(age)s::interval"],
    )
    delete.execute()


class WorkflowQuery(QueryView):

    tables = ['workflow']
    joinmap = {
        'users': 'users ON users.id = workflow.owner',
    }
    fieldmap = {
        'id': ['workflow.id', None],
        'task_id': ['task_id', None],
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
    cls = workflows.get(wf['method'])
    handler = cls(wf)
    try:
        if not handler.check_waits():
            return
            # XXX this doesn't seem like the right place
        handler.run()
    except Exception as err:
        handle_error(job, err)
        raise  # XXX
    update = UpdateProcessor('work_queue', clauses=['id=%(id)s'], values=job)
    update.set(completed=True)
    update.rawset(completion_time='NOW()')
    update.execute()


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

    def run(self):
        if self.data is None:
            self.setup()
            # no steps taken yet
            func = self.start
        else:
            # TODO error handling
            step = self.data['steps'].pop(0)
            func = getattr(self, step)

        # call the next step
        func()

        # update the db
        self.update()

    def setup(self):
        """Called to set up the workflow run"""
        self.data = {'steps': self.get_steps()}

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

    def check_waits(self):
        query = WaitsQuery(
            clauses=[
                ['workflow_id', '=', self.info['id']],
                ['handled', 'IS', False],
            ]
        )
        incomplete = []
        for info in query.execute():
            cls = waits.get(info['method'])
            wait = cls(info)
            if info['fulfilled']:
                # ok, we've been notified
                wait.set_handled()
            elif wait.check():
                # XXX this logic might belong elsewhere
                wait.set_handled()
                # XXX also set fulfilled, but avoid double update
            else:
                incomplete.append(wait)

        return not bool(incomplete)

    def wait_task(self, task_id):
        params = {'task_id': task_id}
        data = {
            'workflow_id': self.info['id'],
            'wait_type': 'task',
            'params': json.dumps(params),
        }
        insert = InsertProcessor('workflow_wait', data=data)
        insert.execute()

    def task(self, method, params, opts=None, wait=True):
        if opts is None:
            opts = {}
        # TODO limit opts?
        opts['parent'] = self.info['task_id']  # XXX
        # we only pass by name
        args = koji.encode_args(**params)
        task_id = kojihub.make_task(method, args, **opts)
        if wait:
            self.wait_task(task_id)

    def start(self):
        raise NotImplementedError('start method not defined')

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
    args = koji.encode_args(method, params)
    task_id = kojihub.make_task('workflow', args, workflow=True)

    # TODO more validation?
    # TODO policy hook
    # TODO callbacks
    data = {
        'id': nextval('workflow_id_seq'),
        'task_id': task_id,
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


waits = SimpleRegistry()


class WaitsQuery(QueryView):

    tables = ['workflow_waits']
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


class TaskWait:

    END_STATES = {koji.TASK_STATES[s] for s in ('CLOSED', 'CANCELED', 'FAILED')}

    def check(self):
        # we also have triggers to update these, but this is a fallback
        params = self.info['params']
        query = QueryProcessor(tables=['task'], columns=['state'],
                               clauses=['id = %(task_id)s'], values=params)
        state = query.singleValue()
        return (state in self.END_STATES)


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
    addWorkflow = staticmethod(add_workflow)
