# kojid plugin

from __future__ import absolute_import

import datetime
import fcntl
import os
import six
import smtplib
import subprocess

import koji
from koji import request_with_retry
import koji.tasks
from koji.tasks import (
    BaseTaskHandler
)
from koji.util import (
    joinpath,
    rmtree
)

__all__ = ('TaskReposTask',)

CONFIG_FILE = '/etc/kojid/plugins/taskrepos.conf'


class TaskReposTask(koji.tasks.BaseTaskHandler):

    Methods = ['taskrepos']
    _taskWeight = 2.0

    def __init__(self, *args, **kwargs):
        self._read_config()
        return super(TaskReposTask, self).__init__(*args, **kwargs)

    def _read_config(self):
        cp = koji.read_config_files(CONFIG_FILE)
        self.config = {
            'REPO_LIFETIME': 14,  # days
        }

        # expire repos in days
        if cp.has_option('taskrepos', 'expire_repos'):
            self.config['REPO_LIFETIME'] = cp.get('taskrepos', 'expire_repos')

    def rm_contents(self, path):
        with os.scandir(path) as it:
            for e in it:
                if e.is_dir(follow_symlinks=False):
                    rmtree(e.path)
                else:
                    os.remove(e.path)

    def requests_get(self, url, filedest):
        self.logger.debug(f'Retrieving {url} to {filedest}...')
        start = datetime.datetime.utcnow()
        resp = request_with_retry().get(url, stream=True)
        try:
            with open(filedest, 'wb') as fo:
                for chunk in resp.iter_content(chunk_size=1048576):
                    fo.write(chunk)
        finally:
            resp.close()
        end = datetime.datetime.utcnow()
        self.logger.debug(f'Received {os.path.getsize(filedest)} bytes in {end - start}')

    def mirror_rpms(self, rpms, content_dir, build=None):
        remote_pi = koji.PathInfo(topdir=self.options.topurl)
        for rpm in rpms:
            if build:
                # rpm is a dict of rpminfo
                url = remote_pi.build(build) + '/' + remote_pi.rpm(rpm)
                dest = joinpath(content_dir, rpm['arch'])
                rpmname = f"{rpm['nvr']}.{rpm['arch']}.rpm"
            else:
                # rpm is a relative path under the work/ directory
                taskdir, rpmname = rpm.split('/')[-2:]
                url = remote_pi.work() + '/' + rpm
                dest = joinpath(content_dir, taskdir)
            koji.ensuredir(dest)
            filedest = joinpath(dest, rpmname)
            self.requests_get(url, filedest)

    def gen_repodata(self, rpmdir, baseurl):
        cmd = [
            '/usr/bin/createrepo_c',
            '--database',
            '--checksum=sha256',
            f'--baseurl={baseurl}',
            '--general-compress-type=gz',
            rpmdir,
        ]
        self.logger.debug(f'Running: {" ".join(cmd)}')

        proc = subprocess.run(cmd, cwd=rpmdir, capture_output=True)
        self.logger.debug(proc.stdout.decode())
        if proc.stderr:
            self.logger.debug(proc.stderr.decode())
        proc.check_returncode()

    def merge_arch_repo(self, arch, repodir, srcdir):
        archdir = joinpath(repodir, arch)
        self.logger.debug(f'Creating {arch} repo under {archdir}')
        koji.ensuredir(archdir)
        cmd = [
            '/usr/bin/mergerepo_c',
            '--koji',
            '--database',
            f'--outputdir={archdir}',
            f'--archlist={arch},noarch,src',
            '--compress-type=gz',
        ]
        for srcrepo in os.listdir(srcdir):
            cmd.append(f'--repo={srcrepo}')
        self.logger.debug(f'Running: {" ".join(cmd)}')
        proc = subprocess.run(cmd, cwd=srcdir, capture_output=True)
        self.logger.debug(proc.stdout.decode())
        if proc.stderr:
            self.logger.debug(proc.stderr.decode())
        proc.check_returncode()

    def build_arches(self, rpms):
        arches = set()
        for rpm in rpms:
            if isinstance(rpm, str):
                rpm = koji.parse_NVRA(os.path.basename(rpm))
            if rpm['arch'] == 'src':
                continue
            arches.add(rpm['arch'])
        return arches

    def create_repos(self, taskinfo):
        builds = self.session.listBuilds(taskID=taskinfo['id'])
        build = None
        task_id = str(self.id)
        if builds:
            build = builds[0]
            rpms = self.session.listRPMs(buildID=build['build_id'])
            repodir = joinpath(self.workdir, 'taskrepos', task_id, task_id,
                               build['name'], build['version'], build['release'])
            nvr = f"{build['name']}.{build['version']}.{build['release']}"
        else:
            children = self.session.getTaskChildren(taskinfo['id'], request=True)
            srpminfo = None
            rpms = []
            for child in children:
                if child['method'] != 'buildArch':
                    continue
                results = self.session.getTaskResult(child['id'])
                if results.get('srpms'):
                    rpms.append(results['srpms'][0])
                    srpmname = os.path.basename(results['srpms'][0])
                    srpminfo = koji.parse_NVRA(srpmname)
                rpms.extend(results['rpms'])
            assert srpminfo, 'missing srpm'
            repodir = joinpath(self.workdir, 'taskrepos', task_id, task_id, 'scratch',
                               srpminfo['name'], srpminfo['version'], srpminfo['release'])
            nvr = f"{srpminfo['name']}.{srpminfo['version']}.{srpminfo['release']}"
        koji.ensuredir(repodir)

        dirfd = os.open(repodir, os.O_RDONLY)
        fcntl.flock(dirfd, fcntl.LOCK_EX)
        self.rm_contents(repodir)
        content_dir = joinpath(repodir, '_content')
        self.mirror_rpms(rpms, content_dir, build=build)
        pi = koji.PathInfo(self.options.topurl)
        for subdir in os.listdir(content_dir):
            if build:
                baseurl = pi.build(build) + '/' + subdir
            else:
                baseurl = pi.task(int(subdir))
            self.gen_repodata(joinpath(content_dir, subdir), baseurl)
        for arch in self.build_arches(rpms):
            self.merge_arch_repo(arch, repodir, content_dir)
        rmtree(content_dir)
        return repodir, baseurl, nvr

    def link_task_repos(self, task_id, repodir):
        """Symlink repodir to $basedir/tasks/{task_id}"""
        tasksdir = joinpath(self.workdir, 'tasks')
        if not os.path.exists(tasksdir):
            koji.ensuredir(tasksdir)
        dstdir = joinpath(tasksdir, str(task_id))
        if os.path.islink(dstdir) or os.path.isfile(dstdir):
            os.remove(dstdir)
        elif os.path.isdir(dstdir):
            rmtree(dstdir)
        os.symlink(os.path.relpath(repodir, tasksdir), dstdir)

    def write_repo_file(self, task, repodir):
        nvr = '-'.join(repodir.split('/')[-3:])
        if len(task['request']) > 2:
            scratch = task['request'][2].get('scratch', False)
        else:
            scratch = False
        subtasks = []
        if task['method'] == 'createrepo':
            subtasks = [subtask for subtask in task.getChildren(task['id'], request=True)]
        repo_arches = []
        for subtask in subtasks:
            repo_arches.append(f'{self.options.topurl}/{subtask["label"]}')
        if 'noarch' in repo_arches:
            repo_url = f'{self.options.topurl}/noarch/'
        else:
            repo_url = f'{self.options.topurl}/$basearch/'
        repo_file_name = nvr
        repo_name = f'brew-task-repo-{nvr.replace('+', '_')}'
        build = 'build'
        if scratch:
            repo_file_name += '-scratch'
            repo_name += '-scratch'
            build = 'scratch build'
        repo_file_name += '.repo'
        repo_file = f'{repodir}/{repo_file_name}'

        with koji._open_text_file(repo_file, 'w') as repo_fd:
            repo_fd.write(
                f"""[{repo_name}]
    name=Repo for Brew {build} of {nvr}
    enabled=1
    gpgcheck=0
    baseurl={repo_url}
    module_hotfixes=1
    """
            )
        self.logger.debug(f'Wrote repo file to {repo_file}')
        return repo_file_name

    def upload_repo(self, repodir):
        repo_files = []
        repo_paths = []
        uploadpath_basic = "taskrepos/%s/%s" % (self.id % 10000, self.id)
        for dirpath, dirs, files in os.walk(repodir):
            relrepodir = os.path.relpath(dirpath, repodir)
            for filename in files:
                path = "%s/%s" % (dirpath, filename)
                if os.path.islink(path):
                    continue
                relpath = "%s/%s" % (relrepodir, filename)
                localpath = '%s/%s' % (repodir, relpath)
                reldir = os.path.dirname(relpath)
                if reldir:
                    uploadpath = "%s/%s" % (uploadpath_basic, reldir)
                    fn = os.path.basename(relpath)
                else:
                    uploadpath = uploadpath_basic
                    fn = relpath
                self.session.uploadWrapper(localpath, uploadpath, fn)
                repo_paths.append(joinpath(uploadpath, filename))
                repo_files.append(relpath)
        return uploadpath_basic, repo_files, repo_paths

    def handler(self, task_id_child, task_link=False):
        if not isinstance(task_id_child, int):
            task_id_child = int(task_id_child)
        taskinfo = self.session.getTaskInfo(task_id_child, request=True)
        policy_data = {
            'user_id': taskinfo['owner'],
        }
        self.session.host.assertPolicy('taskrepos', policy_data)
        if not taskinfo:
            raise koji.BuildError(f'Invalid task ID: {task_id_child}')
        if taskinfo['method'] != 'build':
            raise koji.BuildError(f'{task_id_child} is not a build task')
        build_subtask = next(task for task in self.session.getTaskChildren(task_id_child)
                             if task['method'] == 'buildArch')
        if build_subtask['state'] != 2:
            raise koji.BuildError(f'task {build_subtask['id']} has not completed successfully')
        repodir, baseurl, nvr = self.create_repos(taskinfo)
        self.logger.debug(f'Repos for task {task_id_child} created under {repodir}')
        if task_link:
            self.link_task_repos(task_id_child, repodir)
        self.write_repo_file(taskinfo, repodir)
        uploadpath, repo_files, repo_paths = self.upload_repo(repodir)
        owner_id = taskinfo['owner']
        owner = self.session.getUser(owner_id)['name']
        self.session.tasksRepoNotifications(self.id, repodir, baseurl, owner, repo_paths, nvr)
        return [uploadpath, repo_files]


class tasksRepoNotifications(BaseTaskHandler):
    Methods = ['tasksRepoNotifications']

    _taskWeight = 0.1

    def __init__(self, *args, **kwargs):
        self._read_config()
        return super(tasksRepoNotifications, self).__init__(*args, **kwargs)

    def _read_config(self):
        cp = koji.read_config_files(CONFIG_FILE)
        self.config = {
            'REPO_LIFETIME': 14,  # days
            'TICKETLINK': '',
            'SOURCECODE': '',
        }

        # expire repos in days
        if cp.has_option('taskrepos', 'expire_repos'):
            self.config['REPO_LIFETIME'] = cp.get('taskrepos', 'expire_repos')

        if cp.has_option('taskrepos', 'ticketlink'):
            self.config['TICKETLINK'] = cp.get('taskrepos', 'ticketlink')

        if cp.has_option('taskrepos', 'sourcecodelink'):
            self.config['SOURCECODE'] = cp.get('taskrepos', 'sourcecodelink')

    # XXX externalize these templates somewhere
    subject_templ = 'repo for %(build)s of %(nvr)s is available'
    message_templ = \
        """From: "Brew Task Repos System" %(from_addr)s\r
Subject: %(subject)s\r
To: %(recipient)s\r
A yum repository for the %(build)s of %(nvr)s (task %(task_id)s) is available at:\r
\r
%(repodir)s/\r
\r
You can install the rpms locally by putting this .repo file in your /etc/yum.repos.d/ directory:\r
\r
%(repofile)s\r
\r
It is necessary to have internal Red Hat CA certificates installed on the system, if
you want to use yum repository. You can install required certificates from:\r
\r
* http://hdn.corp.redhat.com/rhel7-csb-stage/repoview/redhat-internal-cert-install.html (rpm)\r
* https://certs.corp.redhat.com/ (cert files)\r
\r
The full list of repos is:\r
%(repo_urls)s\r
\r
The repository will be available for the next %(repolifetime)s days.
Scratch build output will be deleted earlier, based on the Brew scratch build retention policy.\r
\r
If you found a bug or you wish to stop these emails, please create a ticket:\r
%(ticketlink)s\r
\r
Source code at %(sourcecode)s\r
"""

    def handler(self, recipient, repo_urls, task_id, repodir, repofile, nvr):
        if len(recipient) == 0:
            self.logger.debug(f'task {self.id}: no recipients, not sending notifications')
            return
        from_addr = self.options.from_addr
        ticketlink = self.config['TICKETLINK']
        sourcecode = self.config['SOURCECODE']
        repolifetime = self.config['REPO_LIFETIME']
        build = nvr.split('.')[0]
        repo_urls = '\n'.join(repo_urls)
        subject = self.subject_templ % locals()
        message = self.message_templ % locals()

        # ensure message is in UTF-8
        message = koji.fixEncoding(message)
        # binary for python3
        if six.PY3:
            message = message.encode('utf8')
        server = smtplib.SMTP(self.options.smtphost)
        if self.options.smtp_user is not None and self.options.smtp_pass is not None:
            server.login(self.options.smtp_user, self.options.smtp_pass)
        # server.set_debuglevel(True)
        server.sendmail(from_addr, recipient, message)
        server.quit()

        return f'sent notification of taskrepo {self.id} to: {recipient}'
