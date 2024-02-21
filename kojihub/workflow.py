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


def step(order):
    """Decorator to mark a step and indicate order"""
    def decorator(func):
        func.is_step = True
        func.order = order
        return func
    return decorator


class NewRepoWorkFlow:

    def __init__(self, params):
        # XXX probably want to inherit our init, but for now, let's put it here
        self.params = params  # dict
        # TODO: maybe a callback?

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

