from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    ensure_connection,
    get_usage_str,
    printTaskInfo,
)


def anon_handle_taskinfo(goptions, session, args):
    """[info] Show information about a task"""
    usage = "usage: %prog taskinfo [options] <task_id> [<task_id> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-r", "--recurse", action="store_true",
                      help="Show children of this task as well")
    parser.add_option("-v", "--verbose", action="store_true", help="Be verbose")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("You must specify at least one task ID")

    ensure_connection(session, goptions)

    for arg in args:
        task_id = int(arg)
        printTaskInfo(session, task_id, goptions.topdir, 0, options.recurse, options.verbose)
