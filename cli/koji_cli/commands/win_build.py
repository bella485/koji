from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    watch_tasks
)


def handle_win_build(options, session, args):
    """[build] Build a Windows package from source"""
    # Usage & option parsing
    usage = "usage: %prog win-build [options] <target> <URL> <VM>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--winspec", metavar="URL",
                      help="SCM URL to retrieve the build descriptor from. "
                           "If not specified, the winspec must be in the root directory "
                           "of the source repository.")
    parser.add_option("--patches", metavar="URL",
                      help="SCM URL of a directory containing patches to apply "
                           "to the sources before building")
    parser.add_option("--cpus", type="int",
                      help="Number of cpus to allocate to the build VM "
                           "(requires admin access)")
    parser.add_option("--mem", type="int",
                      help="Amount of memory (in megabytes) to allocate to the build VM "
                           "(requires admin access)")
    parser.add_option("--static-mac", action="store_true",
                      help="Retain the original MAC address when cloning the VM")
    parser.add_option("--specfile", metavar="URL",
                      help="SCM URL of a spec file fragment to use to generate wrapper RPMs")
    parser.add_option("--scratch", action="store_true",
                      help="Perform a scratch build")
    parser.add_option("--repo-id", type="int", help="Use a specific repo")
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag package")
    parser.add_option("--background", action="store_true",
                      help="Run the build at a lower priority")
    parser.add_option("--wait", action="store_true",
                      help="Wait on the build, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on build")
    parser.add_option("--quiet", action="store_true",
                      help="Do not print the task information", default=options.quiet)
    (build_opts, args) = parser.parse_args(args)
    if len(args) != 3:
        parser.error(
            "Exactly three arguments (a build target, a SCM URL, and a VM name) are required")
    activate_session(session, options)
    target = args[0]
    if target.lower() == "none" and build_opts.repo_id:
        target = None
        build_opts.skip_tag = True
    else:
        build_target = session.getBuildTarget(target)
        if not build_target:
            parser.error("No such build target: %s" % target)
        dest_tag = session.getTag(build_target['dest_tag'])
        if not dest_tag:
            parser.error("No such destination tag: %s" % build_target['dest_tag_name'])
        if dest_tag['locked'] and not build_opts.scratch:
            parser.error("Destination tag %s is locked" % dest_tag['name'])
    scmurl = args[1]
    vm_name = args[2]
    opts = {}
    for key in ('winspec', 'patches', 'cpus', 'mem', 'static_mac',
                'specfile', 'scratch', 'repo_id', 'skip_tag'):
        val = getattr(build_opts, key)
        if val is not None:
            opts[key] = val
    priority = None
    if build_opts.background:
        # relative to koji.PRIO_DEFAULT
        priority = 5
    task_id = session.winBuild(vm_name, scmurl, target, opts, priority=priority)
    if not build_opts.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if build_opts.wait or (build_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=build_opts.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)
