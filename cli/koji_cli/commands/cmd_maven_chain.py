from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    watch_tasks
)


def handle_maven_chain(options, session, args):
    "[build] Run a set of Maven builds in dependency order"
    usage = "usage: %prog maven-chain [options] <target> <config> [<config> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag builds")
    parser.add_option("--scratch", action="store_true", help="Perform scratch builds")
    parser.add_option("--debug", action="store_true", help="Run Maven build in debug mode")
    parser.add_option("--force", action="store_true", help="Force rebuilds of all packages")
    parser.add_option("--wait", action="store_true",
                      help="Wait on build, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on build")
    parser.add_option("--background", action="store_true",
                      help="Run the build at a lower priority")
    (build_opts, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Two arguments (a build target and a config file) are required")
    activate_session(session, options)
    target = args[0]
    build_target = session.getBuildTarget(target)
    if not build_target:
        parser.error("No such build target: %s" % target)
    dest_tag = session.getTag(build_target['dest_tag'])
    if not dest_tag:
        parser.error("No such destination tag: %s" % build_target['dest_tag_name'])
    if dest_tag['locked'] and not build_opts.scratch:
        parser.error("Destination tag %s is locked" % dest_tag['name'])
    opts = {}
    for key in ('skip_tag', 'scratch', 'debug', 'force'):
        val = getattr(build_opts, key)
        if val:
            opts[key] = val
    try:
        builds = koji.util.parse_maven_chain(args[1:], scratch=opts.get('scratch'))
    except ValueError as e:
        parser.error(e.args[0])
    priority = None
    if build_opts.background:
        priority = 5
    task_id = session.chainMaven(builds, target, opts, priority=priority)
    print("Created task: %d" % task_id)
    print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if build_opts.wait or (build_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


