from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    TimeOption,
    _list_tasks,
    activate_session,
    get_usage_str,
    print_task_headers,
    print_task_recurse
)


def handle_list_tasks(goptions, session, args):
    "[info] Print the list of tasks"
    usage = "usage: %prog list-tasks [options]"
    parser = OptionParser(usage=get_usage_str(usage), option_class=TimeOption)
    parser.add_option("--mine", action="store_true", help="Just print your tasks")
    parser.add_option("--user", help="Only tasks for this user")
    parser.add_option("--arch", help="Only tasks for this architecture")
    parser.add_option("--method", help="Only tasks of this method")
    parser.add_option("--channel", help="Only tasks in this channel")
    parser.add_option("--host", help="Only tasks for this host")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not display the column headers")
    parser.add_option("--before", type="time",
                      help="List tasks completed before this time, " + TimeOption.get_help())
    parser.add_option("--after", type="time",
                      help="List tasks completed after this time (same format as for --before")
    parser.add_option("--all", action="store_true",
                      help="List also finished tasks (valid only with --after)")
    (options, args) = parser.parse_args(args)
    if len(args) != 0:
        parser.error("This command takes no arguments")

    if options.all and not options.after:
        parser.error("--all must be used with --after")

    activate_session(session, goptions)
    tasklist = _list_tasks(options, session)
    if not tasklist:
        print("(no tasks)")
        return
    if not options.quiet:
        print_task_headers()
    for t in tasklist:
        if t.get('sub'):
            # this subtask will appear under another task
            continue
        print_task_recurse(t)
