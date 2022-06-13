from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    _list_tasks,
    activate_session,
    ensure_connection,
    get_usage_str,
    watch_logs
)


def anon_handle_watch_logs(goptions, session, args):
    "[monitor] Watch logs in realtime"
    usage = "usage: %prog watch-logs [options] <task id> [<task id> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--log", help="Watch only a specific log")
    parser.add_option("--mine", action="store_true",
                      help="Watch logs for all your tasks, task_id arguments are forbidden in "
                           "this case.")
    parser.add_option("--follow", action="store_true", help="Follow spawned child tasks")
    (options, args) = parser.parse_args(args)

    if options.mine:
        activate_session(session, goptions)
        if args:
            parser.error("Selection options cannot be combined with a task list")
        tasks = _list_tasks(options, session)
        tasks = [t['id'] for t in tasks]
        if not tasks:
            print("You've no active tasks.")
            return
    else:
        ensure_connection(session, goptions)
        tasks = []
        for task in args:
            try:
                tasks.append(int(task))
            except ValueError:
                parser.error("task id must be an integer")
    if not tasks:
        parser.error("at least one task id must be specified")

    watch_logs(session, tasks, options, goptions.poll_interval)
