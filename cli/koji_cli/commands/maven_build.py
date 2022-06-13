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


def handle_maven_build(options, session, args):
    "[build] Build a Maven package from source"
    usage = "usage: %prog maven-build [options] <target> <URL>"
    usage += "\n       %prog maven-build --ini=CONFIG... [options] <target>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--patches", action="store", metavar="URL",
                      help="SCM URL of a directory containing patches to apply to the sources "
                           "before building")
    parser.add_option("-G", "--goal", action="append", dest="goals", metavar="GOAL", default=[],
                      help="Additional goal to run before \"deploy\"")
    parser.add_option("-P", "--profile", action="append", dest="profiles", metavar="PROFILE",
                      default=[], help="Enable a profile for the Maven build")
    parser.add_option("-D", "--property", action="append", dest="properties", metavar="NAME=VALUE",
                      default=[], help="Pass a system property to the Maven build")
    parser.add_option("-E", "--env", action="append", dest="envs", metavar="NAME=VALUE",
                      default=[], help="Set an environment variable")
    parser.add_option("-p", "--package", action="append", dest="packages", metavar="PACKAGE",
                      default=[], help="Install an additional package into the buildroot")
    parser.add_option("-J", "--jvm-option", action="append", dest="jvm_options", metavar="OPTION",
                      default=[], help="Pass a command-line option to the JVM")
    parser.add_option("-M", "--maven-option", action="append", dest="maven_options",
                      metavar="OPTION", default=[], help="Pass a command-line option to Maven")
    parser.add_option("--ini", action="append", dest="inis", metavar="CONFIG", default=[],
                      help="Pass build parameters via a .ini file")
    parser.add_option("-s", "--section", help="Get build parameters from this section of the .ini")
    parser.add_option("--debug", action="store_true", help="Run Maven build in debug mode")
    parser.add_option("--specfile", action="store", metavar="URL",
                      help="SCM URL of a spec file fragment to use to generate wrapper RPMs")
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag package")
    parser.add_option("--scratch", action="store_true", help="Perform a scratch build")
    parser.add_option("--wait", action="store_true",
                      help="Wait on build, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on build")
    parser.add_option("--quiet", action="store_true",
                      help="Do not print the task information", default=options.quiet)
    parser.add_option("--background", action="store_true",
                      help="Run the build at a lower priority")
    (build_opts, args) = parser.parse_args(args)
    if build_opts.inis:
        if len(args) != 1:
            parser.error("Exactly one argument (a build target) is required")
    else:
        if len(args) != 2:
            parser.error("Exactly two arguments (a build target and a SCM URL) are required")
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
    if build_opts.inis:
        try:
            params = koji.util.parse_maven_param(build_opts.inis, scratch=build_opts.scratch,
                                                 section=build_opts.section)
        except ValueError as e:
            parser.error(e.args[0])
        opts = to_list(params.values())[0]
        if opts.pop('type', 'maven') != 'maven':
            parser.error("Section %s does not contain a maven-build config" %
                         to_list(params.keys())[0])
        source = opts.pop('scmurl')
    else:
        source = args[1]
        opts = koji.util.maven_opts(build_opts, scratch=build_opts.scratch)
    if '://' not in source:
        parser.error("No such SCM URL: %s" % source)
    if build_opts.debug:
        opts.setdefault('maven_options', []).append('--debug')
    if build_opts.skip_tag:
        opts['skip_tag'] = True
    priority = None
    if build_opts.background:
        # relative to koji.PRIO_DEFAULT
        priority = 5
    task_id = session.mavenBuild(source, target, opts, priority=priority)
    if not build_opts.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if build_opts.wait or (build_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=build_opts.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)
