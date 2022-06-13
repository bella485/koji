from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    watch_tasks
)


def handle_tag_build(opts, session, args):
    "[bind] Apply a tag to one or more builds"
    usage = "usage: %prog tag-build [options] <tag> <pkg> [<pkg> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action="store_true", help="force operation")
    parser.add_option("--wait", action="store_true",
                      help="Wait on task, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Do not wait on task")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error(
            "This command takes at least two arguments: a tag name/ID and one or more package "
            "n-v-r's")
    activate_session(session, opts)
    tasks = []
    for pkg in args[1:]:
        task_id = session.tagBuild(args[0], pkg, force=options.force)
        # XXX - wait on task
        tasks.append(task_id)
        print("Created task %d" % task_id)
    if options.wait or (options.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, tasks, quiet=opts.quiet,
                           poll_interval=opts.poll_interval, topurl=opts.topurl)
