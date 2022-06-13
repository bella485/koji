from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    watch_tasks
)


def handle_move_build(opts, session, args):
    "[bind] 'Move' one or more builds between tags"
    usage = "usage: %prog move-build [options] <tag1> <tag2> <pkg> [<pkg> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action="store_true", help="force operation")
    parser.add_option("--wait", action="store_true",
                      help="Wait on tasks, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Do not wait on tasks")
    parser.add_option("--all", action="store_true",
                      help="move all instances of a package, <pkg>'s are package names")
    (options, args) = parser.parse_args(args)
    if len(args) < 3:
        if options.all:
            parser.error(
                "This command, with --all, takes at least three arguments: two tags and one or "
                "more package names")
        else:
            parser.error(
                "This command takes at least three arguments: two tags and one or more package "
                "n-v-r's")
    activate_session(session, opts)
    tasks = []
    builds = []

    if options.all:
        for arg in args[2:]:
            pkg = session.getPackage(arg)
            if not pkg:
                print("No such package: %s, skipping." % arg)
                continue
            tasklist = session.moveAllBuilds(args[0], args[1], arg, options.force)
            tasks.extend(tasklist)
    else:
        for arg in args[2:]:
            build = session.getBuild(arg)
            if not build:
                print("No such build: %s, skipping." % arg)
                continue
            if build not in builds:
                builds.append(build)

        for build in builds:
            task_id = session.moveBuild(args[0], args[1], build['id'], options.force)
            tasks.append(task_id)
            print("Created task %d, moving %s" % (task_id, koji.buildLabel(build)))
    if options.wait or (options.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, tasks, quiet=opts.quiet,
                           poll_interval=opts.poll_interval, topurl=opts.topurl)


