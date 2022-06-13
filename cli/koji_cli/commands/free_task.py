from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_free_task(goptions, session, args):
    "[admin] Free a task"
    usage = "usage: %prog free-task [options] <task_id> [<task_id> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    activate_session(session, goptions)
    tlist = []
    for task_id in args:
        try:
            tlist.append(int(task_id))
        except ValueError:
            parser.error("task_id must be an integer")
    if not tlist:
        parser.error("please specify at least one task_id")
    for task_id in tlist:
        session.freeTask(task_id)
