from __future__ import absolute_import, division

from optparse import OptionParser


import koji
from koji.util import to_list

from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    watch_tasks
)


def handle_wrapper_rpm(options, session, args):
    """[build] Build wrapper rpms for any archives associated with a build."""
    usage = "usage: %prog wrapper-rpm [options] <target> <build-id|n-v-r> <URL>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--create-build", action="store_true",
                      help="Create a new build to contain wrapper rpms")
    parser.add_option("--ini", action="append", dest="inis", metavar="CONFIG", default=[],
                      help="Pass build parameters via a .ini file")
    parser.add_option("-s", "--section",
                      help="Get build parameters from this section of the .ini")
    parser.add_option("--skip-tag", action="store_true",
                      help="If creating a new build, don't tag it")
    parser.add_option("--scratch", action="store_true", help="Perform a scratch build")
    parser.add_option("--wait", action="store_true",
                      help="Wait on build, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on build")
    parser.add_option("--background", action="store_true",
                      help="Run the build at a lower priority")

    (build_opts, args) = parser.parse_args(args)
    if build_opts.inis:
        if len(args) != 1:
            parser.error("Exactly one argument (a build target) is required")
    else:
        if len(args) < 3:
            parser.error("You must provide a build target, a build ID or NVR, "
                         "and a SCM URL to a specfile fragment")
    activate_session(session, options)

    target = args[0]
    if build_opts.inis:
        try:
            params = koji.util.parse_maven_param(build_opts.inis, scratch=build_opts.scratch,
                                                 section=build_opts.section)
        except ValueError as e:
            parser.error(e.args[0])
        opts = to_list(params.values())[0]
        if opts.get('type') != 'wrapper':
            parser.error("Section %s does not contain a wrapper-rpm config" %
                         to_list(params.keys())[0])
        url = opts['scmurl']
        package = opts['buildrequires'][0]
        target_info = session.getBuildTarget(target, strict=True)
        latest_builds = session.getLatestBuilds(target_info['dest_tag'], package=package)
        if not latest_builds:
            parser.error("No build of %s in %s" % (package, target_info['dest_tag_name']))
        build_id = latest_builds[0]['nvr']
    else:
        build_id = args[1]
        if build_id.isdigit():
            build_id = int(build_id)
        url = args[2]
    priority = None
    if build_opts.background:
        priority = 5
    opts = {}
    if build_opts.create_build:
        opts['create_build'] = True
    if build_opts.skip_tag:
        opts['skip_tag'] = True
    if build_opts.scratch:
        opts['scratch'] = True
    task_id = session.wrapperRPM(build_id, url, target, priority, opts=opts)
    print("Created task: %d" % task_id)
    print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if build_opts.wait or (build_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


