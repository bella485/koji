from __future__ import absolute_import, division

import json
import os
import textwrap
from optparse import OptionParser


import koji

from koji_cli.lib import (
    _progress_callback,
    _running_in_bg,
    activate_session,
    get_usage_str,
    unique_path,
    watch_tasks
)


def handle_build(options, session, args):
    "[build] Build a package from source"

    usage = """\
        usage: %prog build [options] <target> <srpm path or scm url>

        The first option is the build target, not to be confused with the destination
        tag (where the build eventually lands) or build tag (where the buildroot
        contents are pulled from).

        You can list all available build targets using the '%prog list-targets' command.
        More detail can be found in the documentation.
        https://docs.pagure.org/koji/HOWTO/#package-organization"""

    usage = textwrap.dedent(usage)
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag package")
    parser.add_option("--scratch", action="store_true", help="Perform a scratch build")
    parser.add_option("--rebuild-srpm", action="store_true", dest="rebuild_srpm",
                      help="Force rebuilding SRPM for scratch build only")
    parser.add_option("--no-rebuild-srpm", action="store_false", dest="rebuild_srpm",
                      help="Force not to rebuild srpm for scratch build only")
    parser.add_option("--wait", action="store_true",
                      help="Wait on the build, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on build")
    parser.add_option("--wait-repo", action="store_true",
                      help="Wait for the actual buildroot repo of given target")
    parser.add_option("--wait-build", metavar="NVR", action="append", dest="wait_builds",
                      default=[], help="Wait for the given nvr to appear in buildroot repo")
    parser.add_option("--quiet", action="store_true",
                      help="Do not print the task information", default=options.quiet)
    parser.add_option("--arch-override", help="Override build arches")
    parser.add_option("--fail-fast", action="store_true",
                      help="Override build_arch_can_fail settings and fail as fast as possible")
    parser.add_option("--repo-id", type="int", help="Use a specific repo")
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")
    parser.add_option("--background", action="store_true",
                      help="Run the build at a lower priority")
    parser.add_option("--custom-user-metadata", type="str",
                      help="Provide a JSON string of custom metadata to be deserialized and "
                           "stored under the build's extra.custom_user_metadata field")
    (build_opts, args) = parser.parse_args(args)
    if len(args) != 2:
        parser.error("Exactly two arguments (a build target and a SCM URL or srpm file) are "
                     "required")
    if build_opts.rebuild_srpm is not None and not build_opts.scratch:
        parser.error("--no-/rebuild-srpm is only allowed for --scratch builds")
    if build_opts.arch_override and not build_opts.scratch:
        parser.error("--arch_override is only allowed for --scratch builds")
    custom_user_metadata = {}
    if build_opts.custom_user_metadata:
        try:
            custom_user_metadata = json.loads(build_opts.custom_user_metadata)
        # Use ValueError instead of json.JSONDecodeError for Python 2 and 3 compatibility
        except ValueError:
            parser.error("--custom-user-metadata is not valid JSON")

    if not isinstance(custom_user_metadata, dict):
        parser.error("--custom-user-metadata must be a JSON object")

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
    source = args[1]
    opts = {}
    if build_opts.arch_override:
        opts['arch_override'] = koji.parse_arches(build_opts.arch_override)
    for key in ('skip_tag', 'scratch', 'repo_id', 'fail_fast', 'wait_repo', 'wait_builds',
                'rebuild_srpm'):
        val = getattr(build_opts, key)
        if val is not None:
            opts[key] = val
    opts["custom_user_metadata"] = custom_user_metadata
    priority = None
    if build_opts.background:
        # relative to koji.PRIO_DEFAULT
        priority = 5
    # try to check that source is an SRPM
    if '://' not in source:
        # treat source as an srpm and upload it
        if not build_opts.quiet:
            print("Uploading srpm: %s" % source)
        serverdir = unique_path('cli-build')
        if _running_in_bg() or build_opts.noprogress or build_opts.quiet:
            callback = None
        else:
            callback = _progress_callback
        session.uploadWrapper(source, serverdir, callback=callback)
        print('')
        source = "%s/%s" % (serverdir, os.path.basename(source))
    task_id = session.build(source, target, opts, priority=priority)
    if not build_opts.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if build_opts.wait or (build_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=build_opts.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


