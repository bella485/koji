# kojid plugin

from __future__ import absolute_import

import os
import six
import smtplib
import subprocess
import time

import koji
import koji.tasks
from koji.tasks import (
    BaseTaskHandler
)
from koji.util import joinpath


__all__ = ('TaskReposTask', 'tasksRepoNotifications')

CONFIG_FILE = '/etc/kojid/plugins/taskrepos.conf'
config = None


def read_config():
    global config
    cp = koji.read_config_files(CONFIG_FILE)
    config = {
        'repo_lifetime': 14,  # days
        'ticketlink': '',
        'sourcecode': '',
        'email_template': '',
    }

    # expire repos in days
    if cp.has_option('taskrepos', 'expire_repos'):
        config['repo_lifetime'] = cp.getint('taskrepos', 'expire_repos')

    if cp.has_option('taskrepos', 'ticketlink'):
        config['ticketlink'] = cp.get('taskrepos', 'ticketlink')

    if cp.has_option('taskrepos', 'sourcecodelink'):
        config['sourcecode'] = cp.get('taskrepos', 'sourcecodelink')

    if cp.has_option('taskrepos', 'email_template'):
        config['email_template'] = cp.get('taskrepos', 'email_template')


class TaskReposTask(koji.tasks.BaseTaskHandler):

    Methods = ['taskrepos']
    _taskWeight = 2.0

    def gen_repodata(self, rpmdir):
        koji.ensuredir(rpmdir)
        cmd = [
            '/usr/bin/createrepo_c',
            '--database',
            '--outputdir=%s' % rpmdir,
            '--checksum=sha256',
            '--general-compress-type=gz',
            '--pkglist=pkglist',
            self.options.topdir,
        ]
        self.logger.debug('Running: %s' % " ".join(cmd))

        proc = subprocess.run(cmd, cwd=rpmdir, capture_output=True)
        self.logger.debug(proc.stdout.decode())
        if proc.stderr:
            self.logger.debug(proc.stderr.decode())
        proc.check_returncode()

    def merge_arch_repo(self, arch, repodir, srcdir):
        archdir = joinpath(repodir, arch)
        self.logger.debug('Creating %s repo under %s' % (arch, archdir))
        koji.ensuredir(archdir)
        cmd = [
            '/usr/bin/mergerepo_c',
            '--koji',
            '--database',
            '--outputdir=%s' % archdir,
            '--archlist=%s,noarch,src' % arch,
            '--compress-type=gz',
        ]
        for srcrepo in os.listdir(srcdir):
            cmd.append('--repo=%s' % srcrepo)
        self.logger.debug('Running: %s' " ".join(cmd))
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

    def create_pkg_list(self, rpms, repodir, build=None):
        remote_pi = koji.PathInfo(topdir=self.options.topdir)
        dest_dirs = []
        pkglist = []
        for rpm in rpms:
            if build:
                # rpm is a dict of rpminfo
                url = remote_pi.build(build) + '/' + remote_pi.rpm(rpm)
                dest = joinpath(repodir, rpm['arch'])
            else:
                # rpm is a relative path under the work/ directory
                taskdir, rpmname = rpm.split('/')[-2:]
                url = joinpath(remote_pi.work(), rpm).replace(self.options.topdir, '', 1)
                rpminfo = self.session.getRPM(rpmname)
                count = 0
                while rpminfo is None and count != 20:
                    time.sleep(2)
                    rpminfo = self.session.getRPM(rpmname)
                    count += 1
                if rpminfo['arch'] != 'src':
                    dest = joinpath(repodir, rpminfo['arch'])
                else:
                    continue
            if dest not in dest_dirs:
                dest_dirs.append(dest)
            pkglist.append(url)
        for dest_dir in dest_dirs:
            os.makedirs(dest_dir, exist_ok=True)
            os.symlink(koji.pathinfo.topdir, joinpath(dest_dir, 'toplink'))
            with open(joinpath(dest_dir, 'pkglist'), 'w') as dd:
                for pkg in pkglist:
                    dd.write(pkg)
            with open(joinpath(dest_dir, 'pkglist'), 'r') as dd:
                for line in dd:
                    self.logger.info('PkgLINE %s' % line)
        return dest_dirs

    def create_repos(self, taskinfo):
        builds = self.session.listBuilds(taskID=taskinfo['id'])
        build = None
        if builds:
            info = builds[0]
            rpms = self.session.listRPMs(buildID=info['build_id'])
            repotype = 'official'
        else:
            info = None
            rpms = []
            results = self.session.getTaskResult(taskinfo['id'])
            if results.get('srpms'):
                rpms.append(results['srpms'][0])
                srpmname = os.path.basename(results['srpms'][0])
                info = koji.parse_NVRA(srpmname)
            rpms.extend(results['rpms'])
            if info is None:
                raise koji.GenericError("SRPM is missing.")
            repotype = 'scratch'
        repodir = joinpath(self.workdir, info['name'], info['version'], info['release'])
        repodir_nvr = joinpath(repotype, info['name'], info['version'], info['release'])
        nvr = '%s-%s-%s' % (info['name'], info['version'], info['release'])

        koji.ensuredir(repodir)

        self.create_pkg_list(rpms, repodir, build=build)
        for arch in self.build_arches(rpms):
            self.gen_repodata(joinpath(repodir, arch))
        for arch in self.build_arches(rpms):
            self.merge_arch_repo(arch, repodir, repodir)
        return repodir, repodir_nvr, nvr, repotype

    def write_repo_file(self, task, repodir, nvr, repotype):
        if len(task['request']) > 2:
            for tr in task['request']:
                if isinstance(tr, dict):
                    scratch = tr.get('scratch', False)
        else:
            scratch = False
        subtasks = []
        if task['method'] == 'createrepo':
            subtasks = [subtask for subtask in task.getChildren(task['id'], request=True)]
        repo_arches = []
        for subtask in subtasks:
            repo_arches.append('%s/%s' % (self.options.topurl, subtask["label"]))
        rpminfo = koji.parse_NVR(nvr)
        baseurl = joinpath(self.options.topurl, 'repos-tasks', repotype,
                           rpminfo['name'], rpminfo['version'], rpminfo['release'])
        if 'noarch' in repo_arches:
            baseurl = '%s/noarch/' % baseurl
        else:
            baseurl = '%s/%s/' % (baseurl, '$basearch')
        repo_file_name = nvr
        repo_name = 'brew-task-repo-%s' % nvr.replace('+', '_')
        build = 'build'
        if scratch:
            repo_file_name += '-scratch'
            repo_name += '-scratch'
            build = 'scratch build'
        repo_file_name += '.repo'
        repo_file = '%s/%s' % (repodir, repo_file_name)

        with koji._open_text_file(repo_file, 'w') as repo_fd:
            repo_fd.write(
                """[%s]
name=Repo for Brew %s of %s
enabled=1
gpgcheck=0
baseurl=%s
module_hotfixes=1
""" % (repo_name, build, nvr, baseurl)
            )
        self.logger.debug('Wrote repo file to %s' % repo_file)
        return baseurl

    def upload_repo(self, repodir):
        repo_files = []
        repos = []
        repopath_part = '/'.join(repodir.split('/')[-4:])
        uploadpath_basic = joinpath("repos-tasks", repopath_part)
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
                repos.append([uploadpath, fn])
                repo_files.append(relpath)
        return repo_files, repos, repopath_part

    def handler(self, task_id_child, task_link=False):
        read_config()
        data = {}
        if not isinstance(task_id_child, int):
            task_id_child = int(task_id_child)
        taskinfo = self.session.getTaskInfo(task_id_child, request=True)
        policy_data = {
            'user_id': taskinfo['owner'],
        }
        self.session.host.assertPolicy('taskrepos', policy_data)
        if not taskinfo:
            raise koji.BuildError('Invalid task ID: %s' % task_id_child)
        if taskinfo['method'] != 'buildArch':
            raise koji.BuildError('%s is not a build task' % task_id_child)
        if taskinfo['state'] != 2:
            raise koji.BuildError('task %s has not completed successfully' % taskinfo['id'])
        repodir, repodir_nvr, data['nvr'], repotype = self.create_repos(taskinfo)
        self.logger.debug('Repos for task %s created under %s' % (task_id_child, repodir))
        data['baseurl'] = self.write_repo_file(taskinfo, repodir, data['nvr'], repotype)
        repo_files, repos, repopath_part = self.upload_repo(repodir)
        data['owner'] = self.session.getUser(taskinfo['owner'])['name']
        data['repo_urls'], uploadpath = self.session.taskRepoDone(
            self.id, repos, repodir_nvr, self.options.topurl, task_link)
        data['repodir'] = joinpath(koji.pathinfo.topdir, 'repos-tasks', repodir_nvr)
        self.session.host.tasksRepoNotifications(self.id, data)
        return [uploadpath, repo_files]


class tasksRepoNotifications(BaseTaskHandler):
    Methods = ['tasksRepoNotifications']
    _taskWeight = 0.1
    INI_FILE = '/etc/kojid/plugins/taskrepos.ini'
    ini_config = ''

    def read_ini_file(self):
        cp_ini = koji.read_config_files(self.INI_FILE)
        self.ini_config = {
            'email_template': '',
        }

        # expire repos in days
        if cp_ini.has_option('EMAIL TEMPLATE', 'email_template'):
            self.ini_config['email_template'] = cp_ini.get('EMAIL TEMPLATE', 'email_template')

    def handler(self, recipient, data):
        read_config()
        self.read_ini_file()
        data['from_addr'] = self.options.from_addr
        repo_urls = data['repo_urls'].copy()
        data['repo_urls'] = ''
        for rf in repo_urls:
            if '.repo' in rf:
                data['repofile'] = rf
            else:
                data['repo_urls'] += '%s\n' % rf
        data['ticketlink'] = config['ticketlink']
        data['sourcecode'] = config['sourcecode']
        data['repolifetime'] = config['repo_lifetime']
        data['build'] = '-'.join(data['nvr'].split('-')[:-2])
        data['recipient'] = recipient
        message = config['email_template'] % data

        # ensure message is in UTF-8
        message = koji.fixEncoding(message)
        # binary for python3
        if six.PY3:
            message = message.encode('utf8')
        server = smtplib.SMTP(self.options.smtphost)
        if self.options.smtp_user is not None and self.options.smtp_pass is not None:
            server.login(self.options.smtp_user, self.options.smtp_pass)
        # server.set_debuglevel(True)
        server.sendmail(data['from_addr'], recipient, message)
        server.quit()

        return 'sent notification of taskrepo %s to: %s' % (self.id, recipient)
