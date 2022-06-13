from __future__ import absolute_import, division

import os
import pprint
import sys
import traceback
from optparse import OptionParser

import six
import six.moves.xmlrpc_client

import koji

from koji_cli.lib import (
    download_file,
    ensure_connection,
    error,
    get_usage_str,
    list_task_output_all_volumes,
    warn
)


def anon_handle_download_logs(options, session, args):
    "[download] Download logs for task"

    FAIL_LOG = "task_failed.log"
    usage = "usage: %prog download-logs [options] <task_id> [<task_id> ...]"
    usage += "\n       %prog download-logs [options] --nvr <n-v-r> [<n-v-r> ...]"
    usage += "\n"
    usage += "\n"
    usage += "Note this command only downloads task logs, not build logs."
    usage += "\n"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-r", "--recurse", action="store_true",
                      help="Process children of this task as well")
    parser.add_option("--nvr", action="store_true",
                      help="Get the logs for the task associated with this build "
                           "Name-Version-Release.")
    parser.add_option("-m", "--match", action="append", metavar="PATTERN",
                      help="Get only log filenames matching PATTERN (fnmatch). "
                           "May be used multiple times.")
    parser.add_option("-c", "--continue", action="store_true", dest="cont",
                      help="Continue previous download")
    parser.add_option("-d", "--dir", metavar="DIRECTORY", default='kojilogs',
                      help="Write logs to DIRECTORY")
    (suboptions, args) = parser.parse_args(args)

    if len(args) < 1:
        parser.error("Please specify at least one task id or n-v-r")

    def write_fail_log(task_log_dir, task_id):
        """Gets output only from failed tasks"""
        try:
            result = session.getTaskResult(task_id)
            # with current code, failed task results should always be faults,
            # but that could change in the future
            content = pprint.pformat(result)
        except (six.moves.xmlrpc_client.Fault, koji.GenericError):
            etype, e = sys.exc_info()[:2]
            content = ''.join(traceback.format_exception_only(etype, e))
        full_filename = os.path.normpath(os.path.join(task_log_dir, FAIL_LOG))
        koji.ensuredir(os.path.dirname(full_filename))
        sys.stdout.write("Writing: %s\n" % full_filename)
        with open(full_filename, 'wt') as fo:
            fo.write(content)

    def download_log(task_log_dir, task_id, filename, blocksize=102400, volume=None):
        # Create directories only if there is any log file to write to
        # For each non-default volume create special sub-directory
        if volume not in (None, 'DEFAULT'):
            full_filename = os.path.normpath(os.path.join(task_log_dir, volume, filename))
        else:
            full_filename = os.path.normpath(os.path.join(task_log_dir, filename))
        koji.ensuredir(os.path.dirname(full_filename))
        contents = 'IGNORE ME!'
        if suboptions.cont and os.path.exists(full_filename):
            sys.stdout.write("Continuing: %s\n" % full_filename)
            fd = open(full_filename, 'ab')
            offset = fd.tell()
        else:
            sys.stdout.write("Downloading: %s\n" % full_filename)
            fd = open(full_filename, 'wb')
            offset = 0
        try:
            while contents:
                contents = session.downloadTaskOutput(task_id, filename, offset=offset,
                                                      size=blocksize, volume=volume)
                offset += len(contents)
                if contents:
                    fd.write(contents)
        finally:
            fd.close()

    def save_logs(task_id, match, parent_dir='.', recurse=True):
        assert task_id == int(task_id), "Task id must be number: %r" % task_id
        task_info = session.getTaskInfo(task_id)
        if task_info is None:
            error("No such task: %d" % task_id)
        files = list_task_output_all_volumes(session, task_id)
        logs = []  # list of tuples (filename, volume)
        for filename in files:
            if not filename.endswith(".log"):
                continue
            if match and not koji.util.multi_fnmatch(filename, match):
                continue
            logs += [(filename, volume) for volume in files[filename]]

        task_log_dir = os.path.join(parent_dir,
                                    "%s-%s" % (task_info["arch"], task_id))

        count = 0
        state = koji.TASK_STATES[task_info['state']]
        if state == 'FAILED':
            if not match or koji.util.multi_fnmatch(FAIL_LOG, match):
                write_fail_log(task_log_dir, task_id)
                count += 1
        elif state not in ['CLOSED', 'CANCELED']:
            warn("Task %s is %s\n" % (task_id, state))

        for log_filename, log_volume in logs:
            download_log(task_log_dir, task_id, log_filename, volume=log_volume)
            count += 1

        if count == 0 and not recurse:
            warn("No logs found for task %i. Perhaps try --recurse?\n" % task_id)

        if recurse:
            child_tasks = session.getTaskChildren(task_id)
            for child_task in child_tasks:
                save_logs(child_task['id'], match, task_log_dir, recurse)

    ensure_connection(session, options)
    task_id = None
    build_id = None
    for arg in args:
        if suboptions.nvr:
            suboptions.recurse = True
            binfo = session.getBuild(arg)
            if binfo is None:
                error("There is no build with n-v-r: %s" % arg)
            if binfo.get('task_id'):
                task_id = binfo['task_id']
                sys.stdout.write("Using task ID: %s\n" % task_id)
            elif binfo.get('build_id'):
                build_id = binfo['build_id']
                sys.stdout.write("Using build ID: %s\n" % build_id)
        else:
            try:
                task_id = int(arg)
            except ValueError:
                error("Task id must be number: %r" % arg)
        if task_id:
            save_logs(task_id, suboptions.match, suboptions.dir, suboptions.recurse)
        elif build_id:
            logs = session.getBuildLogs(build_id)
            match = suboptions.match
            for log in logs:
                url = os.path.join(options.topurl, log['path'])
                filepath = os.path.join(os.getcwd(), '%s/%s/%s' % (suboptions.dir,
                                                                   arg, log['name']))
                if not filepath.endswith(".log"):
                    continue
                if match and not koji.util.multi_fnmatch(log['name'], match):
                    continue
                download_file(url, filepath)
