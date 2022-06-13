from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    error,
    get_usage_str,
    warn,
    watch_tasks
)


def handle_chain_build(options, session, args):
    # XXX - replace handle_build with this, once chain-building has gotten testing
    "[build] Build one or more packages from source"
    usage = "usage: %prog chain-build [options] <target> <URL> [<URL> [:] <URL> [:] <URL> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--wait", action="store_true",
                      help="Wait on build, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on build")
    parser.add_option("--quiet", action="store_true",
                      help="Do not print the task information", default=options.quiet)
    parser.add_option("--background", action="store_true",
                      help="Run the build at a lower priority")
    (build_opts, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("At least two arguments (a build target and a SCM URL) are required")
    activate_session(session, options)
    target = args[0]
    build_target = session.getBuildTarget(target)
    if not build_target:
        parser.error("No such build target: %s" % target)
    dest_tag = session.getTag(build_target['dest_tag'], strict=True)
    if dest_tag['locked']:
        parser.error("Destination tag %s is locked" % dest_tag['name'])

    # check that the destination tag is in the inheritance tree of the build tag
    # otherwise there is no way that a chain-build can work
    ancestors = session.getFullInheritance(build_target['build_tag'])
    if dest_tag['id'] not in [build_target['build_tag']] + \
            [ancestor['parent_id'] for ancestor in ancestors]:
        warn("Packages in destination tag %(dest_tag_name)s are not inherited by build tag "
             "%(build_tag_name)s" % build_target)
        error("Target %s is not usable for a chain-build" % build_target['name'])
    sources = args[1:]

    src_list = []
    build_level = []
    # src_lists is a list of lists of sources to build.
    #  each list is block of builds ("build level") which must all be completed
    #  before the next block begins. Blocks are separated on the command line with ':'
    for src in sources:
        if src == ':':
            if build_level:
                src_list.append(build_level)
                build_level = []
        elif '://' in src:
            # quick check that src might be a url
            build_level.append(src)
        elif '/' not in src and not src.endswith('.rpm') and len(src.split('-')) >= 3:
            # quick check that it looks like a N-V-R
            build_level.append(src)
        else:
            error('"%s" is not a SCM URL or package N-V-R' % src)
    if build_level:
        src_list.append(build_level)

    if len(src_list) < 2:
        parser.error('You must specify at least one dependency between builds with : (colon)\n'
                     'If there are no dependencies, use the build command instead')

    priority = None
    if build_opts.background:
        # relative to koji.PRIO_DEFAULT
        priority = 5

    task_id = session.chainBuild(src_list, target, priority=priority)
    if not build_opts.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if build_opts.wait or (build_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=build_opts.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


