# koji hub plugin
# There is a kojid plugin that goes with this hub plugin. The kojid builder
# plugin has a config file.  This hub plugin has no config file.


import kojihub
import logging
import os
import shutil

import koji
from koji.context import context
from koji.plugin import export, export_in, callback
from koji.util import joinpath, safer_move, rmtree

__all__ = ('taskrepos', 'tasksRepoNotifications', 'taskRepoDone')


logger = logging.getLogger('koji.hub')


@export
def taskRepoDone(task_id, repos, repodir_nvr, topurl, task_link):
    repo_urls = []
    nvr_dir = '/'.join(repodir_nvr.split('/')[1:]) + '/'
    for rp, fn in repos:
        src = joinpath(koji.pathinfo.work(), rp, fn).replace('/.', '/')
        repostasks_dir = joinpath(koji.pathinfo.topdir, 'repos-tasks', repodir_nvr,
                                  rp.split(nvr_dir)[1]).replace('/.', '/')
        repostasks_file = joinpath(repostasks_dir, fn).replace('/.', '/')
        koji.ensuredir(repostasks_dir)
        if not os.path.exists(src):
            raise koji.GenericError("uploaded file missing: %s" % src)
        safer_move(src, repostasks_file)
        repo_urls.append(joinpath(topurl, 'repos-tasks', repodir_nvr,
                                  rp.split(nvr_dir)[1], fn).replace('/.', '/'))
    if task_link:
        repodir = joinpath(koji.pathinfo.topdir, 'repos-tasks', repodir_nvr)
        task_dir_with_id = joinpath(koji.pathinfo.topdir, 'tasks', str(task_id % 10000))
        koji.ensuredir(task_dir_with_id)
        dstdir = joinpath(task_dir_with_id, str(task_id))
        if os.path.islink(dstdir) or os.path.isfile(dstdir):
            os.remove(dstdir)
        elif os.path.isdir(dstdir):
            rmtree(dstdir)
        os.symlink(repodir, dstdir)
    shutil.rmtree(joinpath(
        koji.pathinfo.work(), '/'.join(rp.split(str(task_id))[:1]), str(task_id)))
    return repo_urls, repostasks_dir


@callback('postTaskStateChange')
def taskrepos(cbtype, *args, **kws):
    if kws['attribute'] != 'state':
        return
    task_id = kws['info']['id']
    task = kojihub.Task(task_id)
    taskinfo = task.getInfo()
    if taskinfo['method'] != 'buildArch' or taskinfo['state'] != koji.TASK_STATES['CLOSED']:
        return
    task_link = True

    kojihub.make_task('taskrepos', [task_id, task_link])


@export_in('host')
def tasksRepoNotifications(task_id, data):
    if context.opts.get('DisableNotifications'):
        return
    email_domain = context.opts['EmailDomain']
    email = f'{data['owner']}@{email_domain}'

    data['task_id'] = task_id
    kojihub.make_task('tasksRepoNotifications', [email, data])
