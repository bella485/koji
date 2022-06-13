from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_set_task_priority(goptions, session, args):
    "[admin] Set task priority"
    usage = "usage: %prog set-task-priority [options] --priority=<priority> <task_id> " \
            "[<task_id> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--priority", type="int", help="New priority")
    parser.add_option("--recurse", action="store_true", default=False,
                      help="Change priority of child tasks as well")
    (options, args) = parser.parse_args(args)
    if len(args) == 0:
        parser.error("You must specify at least one task id")

    if options.priority is None:
        parser.error("You must specify --priority")
    try:
        tasks = [int(a) for a in args]
    except ValueError:
        parser.error("Task numbers must be integers")

    activate_session(session, goptions)

    if not session.hasPerm('admin'):
        logged_user = session.getLoggedInUser()
        error("admin permission required (logged in as %s)" % logged_user['name'])

    for task_id in tasks:
        try:
            session.setTaskPriority(task_id, options.priority, options.recurse)
        except koji.GenericError:
            warn("Can't update task priority on closed task: %s" % task_id)


