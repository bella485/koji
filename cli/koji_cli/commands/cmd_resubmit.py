from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    watch_tasks
)


def handle_resubmit(goptions, session, args):
    """[build] Retry a canceled or failed task, using the same parameter as the original task."""
    usage = "usage: %prog resubmit [options] <task_id>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--wait", action="store_true",
                      help="Wait on task, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on task")
    parser.add_option("--nowatch", action="store_true", dest="nowait",
                      help="An alias for --nowait")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the task information")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Please specify a single task ID")
    activate_session(session, goptions)
    taskID = int(args[0])
    if not options.quiet:
        print("Resubmitting the following task:")
        _printTaskInfo(session, taskID, goptions.topdir, 0, False, True)
    newID = session.resubmitTask(taskID)
    if not options.quiet:
        print("Resubmitted task %s as new task %s" % (taskID, newID))
    if options.wait or (options.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [newID], quiet=options.quiet,
                           poll_interval=goptions.poll_interval, topurl=goptions.topurl)


