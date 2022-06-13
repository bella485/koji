from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    _list_tasks,
    activate_session,
    ensure_connection,
    get_usage_str,
    watch_tasks
)


def anon_handle_watch_task(goptions, session, args):
    "[monitor] Track progress of particular tasks"
    usage = "usage: %prog watch-task [options] <task id> [<task id> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the task information")
    parser.add_option("--mine", action="store_true", help="Just watch your tasks")
    parser.add_option("--user", help="Only tasks for this user")
    parser.add_option("--arch", help="Only tasks for this architecture")
    parser.add_option("--method", help="Only tasks of this method")
    parser.add_option("--channel", help="Only tasks in this channel")
    parser.add_option("--host", help="Only tasks for this host")
    (options, args) = parser.parse_args(args)
    selection = (options.mine or
                 options.user or
                 options.arch or
                 options.method or
                 options.channel or
                 options.host)
    if args and selection:
        parser.error("Selection options cannot be combined with a task list")

    if options.mine:
        activate_session(session, goptions)
    else:
        ensure_connection(session, goptions)
    if selection:
        tasks = [task['id'] for task in _list_tasks(options, session)]
        if not tasks:
            print("(no tasks)")
            return
    else:
        tasks = []
        for task in args:
            try:
                tasks.append(int(task))
            except ValueError:
                parser.error("task id must be an integer")
        if not tasks:
            parser.error("at least one task id must be specified")

    return watch_tasks(session, tasks, quiet=options.quiet,
                       poll_interval=goptions.poll_interval, topurl=goptions.topurl)


