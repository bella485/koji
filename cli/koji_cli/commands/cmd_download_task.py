from __future__ import absolute_import, division

import os
from optparse import OptionParser

from six.moves import filter

import koji

from koji_cli.lib import (
    _running_in_bg,
    download_file,
    ensure_connection,
    error,
    get_usage_str,
    list_task_output_all_volumes,
    watch_tasks
)


def anon_handle_download_task(options, session, args):
    "[download] Download the output of a build task"
    usage = "usage: %prog download-task <task_id>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arch", dest="arches", metavar="ARCH", action="append", default=[],
                      help="Only download packages for this arch (may be used multiple times)")
    parser.add_option("--logs", dest="logs", action="store_true", default=False,
                      help="Also download build logs")
    parser.add_option("--topurl", metavar="URL", default=options.topurl,
                      help="URL under which Koji files are accessible")
    parser.add_option("--noprogress", action="store_true", help="Do not display progress meter")
    parser.add_option("--wait", action="store_true",
                      help="Wait for running tasks to finish, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Do not wait for running tasks to finish")
    parser.add_option("-q", "--quiet", action="store_true",
                      help="Suppress output", default=options.quiet)

    (suboptions, args) = parser.parse_args(args)
    if len(args) == 0:
        parser.error("Please specify a task ID")
    elif len(args) > 1:
        parser.error("Only one task ID may be specified")

    base_task_id = int(args.pop())
    if len(suboptions.arches) > 0:
        suboptions.arches = ",".join(suboptions.arches).split(",")

    ensure_connection(session, options)

    # get downloadable tasks

    base_task = session.getTaskInfo(base_task_id)
    if not base_task:
        error('No such task: %d' % base_task_id)

    if (suboptions.wait or (suboptions.wait is None and not _running_in_bg())) and \
            base_task['state'] not in (
            koji.TASK_STATES['CLOSED'],
            koji.TASK_STATES['CANCELED'],
            koji.TASK_STATES['FAILED']):
        watch_tasks(session, [base_task_id], quiet=suboptions.quiet,
                    poll_interval=options.poll_interval, topurl=options.topurl)

    def check_downloadable(task):
        return task["method"] == "buildArch"

    downloadable_tasks = []

    if check_downloadable(base_task):
        downloadable_tasks.append(base_task)
    else:
        subtasks = session.getTaskChildren(base_task_id)
        downloadable_tasks.extend(list(filter(check_downloadable, subtasks)))

    # get files for download
    downloads = []

    for task in downloadable_tasks:
        files = list_task_output_all_volumes(session, task["id"])
        for filename in files:
            if filename.endswith(".rpm"):
                for volume in files[filename]:
                    filearch = filename.split(".")[-2]
                    if len(suboptions.arches) == 0 or filearch in suboptions.arches:
                        downloads.append((task, filename, volume, filename))
            elif filename.endswith(".log") and suboptions.logs:
                for volume in files[filename]:
                    # rename logs, they would conflict
                    new_filename = "%s.%s.log" % (filename.rstrip(".log"), task["arch"])
                    downloads.append((task, filename, volume, new_filename))

    if len(downloads) == 0:
        error("No files for download found.")

    required_tasks = {}
    for (task, nop, nop, nop) in downloads:
        if task["id"] not in required_tasks:
            required_tasks[task["id"]] = task

    for task_id in required_tasks:
        if required_tasks[task_id]["state"] != koji.TASK_STATES.get("CLOSED"):
            if task_id == base_task_id:
                error("Task %d has not finished yet." % task_id)
            else:
                error("Child task %d has not finished yet." % task_id)

    # perform the download
    number = 0
    pathinfo = koji.PathInfo(topdir=suboptions.topurl)
    for (task, filename, volume, new_filename) in downloads:
        number += 1
        if volume not in (None, 'DEFAULT'):
            koji.ensuredir(volume)
            new_filename = os.path.join(volume, new_filename)
        if '..' in filename:
            error('Invalid file name: %s' % filename)
        url = '%s/%s/%s' % (pathinfo.work(volume), pathinfo.taskrelpath(task["id"]), filename)
        download_file(url, new_filename, quiet=suboptions.quiet, noprogress=suboptions.noprogress,
                      size=len(downloads), num=number)


