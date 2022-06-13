from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import map


from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    arg_filter,
    get_usage_str,
    watch_tasks
)


def handle_make_task(goptions, session, args):
    "[admin] Create an arbitrary task"
    usage = "usage: %prog make-task [options] <method> [<arg> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--channel", help="set channel")
    parser.add_option("--priority", help="set priority")
    parser.add_option("--watch", action="store_true", help="watch the task")
    parser.add_option("--arch", help="set arch")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify task method at least")

    activate_session(session, goptions)
    taskopts = {}
    for key in ('channel', 'priority', 'arch'):
        value = getattr(options, key, None)
        if value is not None:
            taskopts[key] = value
    task_id = session.makeTask(method=args[0],
                               arglist=list(map(arg_filter, args[1:])),
                               **taskopts)
    print("Created task id %d" % task_id)
    if _running_in_bg() or not options.watch:
        return
    else:
        session.logout()
        return watch_tasks(session, [task_id], quiet=goptions.quiet,
                           poll_interval=goptions.poll_interval, topurl=goptions.topurl)
