import json
import logging
import time

import koji
from koji.context import context
from . import kojihub
from .db import QueryProcessor, InsertProcessor, UpsertProcessor, UpdateProcessor, \
    DeleteProcessor, QueryView, db_lock


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
        handle_job(job)
        if n >= maxjobs:
            break
        if time.time() - start >= maxtime:
            break


class WorkflowQuery(QueryView):

    tables = ['workflow']
    joinmap = {
        'users': 'users ON users.id = workflow.owner',
    }
    fieldmap = {
        'id': ['workflow.id', None],
        'started': ['workflow.started', None],
        'completed': ['workflow.completed', None],
        'create_time': ['workflow.create_time', None],
        'start_time': ['workflow.start_time', None],
        'completion_time': ['workflow.completion_time', None],
        'create_ts': ["date_part('epoch', workflow.create_time)", None],
        'start_ts': ["date_part('epoch', workflow.start_time)", None],
        'completion_ts': ["date_part('epoch', workflow.completion_time)", None],
        'owner': ['workflow.owner', None],
        'owner_name': ['users.name', 'users'],
        'method': ['workflow.method', None],
        'params', ['workflow.params', None],
        'result', ['workflow.result', None],
        'data', ['workflow.data', None],
    }


def handle_job(job):
    wf = WorkflowQuery(clauses=[['id'], '=', job['workflow_id']]).executeOne(strict=True)
    cls = registry.get(wf['method'])
    handler = cls(wf)
    handler.run()


class WorkflowRegistry:

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


registry = WorkflowRegistry()


def step(order):
    """Decorator to mark a step and indicate order"""
    def decorator(func):
        func.is_step = True
        func.order = order
        return func
    return decorator


class BaseWorkflow:

    def __init__(self, params):
        self.params = params
        # TODO: maybe a callback?


@registry.add('new-repo')
class NewRepoWorkflow(BaseWorkflow):

    @step(1)
    def startup(self):
        # TODO validate params
        kw = self.params
        # ??? should we call repo_init ourselves?
        task_id = self.task('initRepo', **kw)
        self.wait_task(task_id)
        # TODO mechanism for task_id value to persist to next step

    @step(2)
    def repos(self):
        # TODO fetch archlist from task
        repo_tasks = []
        for arch in self.needed_arches:
            args = [repo_id, arch, oldrepo]
            repo_tasks[arch] = self.task('createrepo', *args)
            self.wait_task(repo_tasks[arch])

    @step(3)
    def finalize(self):
        # TODO fetch params from self/tasks
        repo_done(...)

