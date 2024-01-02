# koji hub plugin
# There is a kojid plugin that goes with this hub plugin. The kojid builder
# plugin has a config file.  This hub plugin has no config file.


import kojihub

from koji.context import context
from koji.plugin import export, callback
from koji.util import joinpath

__all__ = ('taskrepos',)


@callback('postBuildStateChange')
def taskrepos(cbtype, *args, **kws):
    task_id = kws['info']['task_id']
    task = kojihub.Task(task_id)
    subtasks_method = [subtask['method'] for subtask in task.getChildren(task_id)]
    if 'buildArch' not in subtasks_method:
        return
    task_link = False

    kojihub.make_task('taskrepos', [task_id, task_link])
    return


@export
def tasksRepoNotifications(task_id, repodir, baseurl, owner, repo_paths, nvr):
    if context.opts.get('DisableNotifications'):
        return
    email_domain = context.opts['EmailDomain']
    email = f'{owner}@{email_domain}'
    repo_urls = []
    for rf in repo_paths:
        if '.repo' in rf:
            repofile = joinpath(baseurl, rf)
        else:
            repo_urls.append(joinpath(baseurl, rf))
    kojihub.make_task('tasksRepoNotifications',
                      [email, repo_urls, task_id, repodir, repofile, nvr])
