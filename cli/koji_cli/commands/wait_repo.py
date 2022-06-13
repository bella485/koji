from __future__ import absolute_import, division

import time
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str,
    warn
)


def anon_handle_wait_repo(options, session, args):
    "[monitor] Wait for a repo to be regenerated"
    usage = "usage: %prog wait-repo [options] <tag>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--build", metavar="NVR", dest="builds", action="append", default=[],
                      help="Check that the given build is in the newly-generated repo "
                           "(may be used multiple times)")
    parser.add_option("--target", action="store_true",
                      help="Interpret the argument as a build target name")
    parser.add_option("--timeout", type="int", default=120,
                      help="Amount of time to wait (in minutes) before giving up "
                           "(default: 120)")
    parser.add_option("--quiet", action="store_true", default=options.quiet,
                      help="Suppress output, success or failure will be indicated by the return "
                           "value only")
    (suboptions, args) = parser.parse_args(args)

    start = time.time()

    builds = [koji.parse_NVR(build) for build in suboptions.builds]
    if len(args) < 1:
        parser.error("Please specify a tag name")
    elif len(args) > 1:
        parser.error("Only one tag may be specified")

    tag = args[0]

    ensure_connection(session, options)
    if suboptions.target:
        target_info = session.getBuildTarget(tag)
        if not target_info:
            parser.error("No such build target: %s" % tag)
        tag = target_info['build_tag_name']
        tag_id = target_info['build_tag']
    else:
        tag_info = session.getTag(tag)
        if not tag_info:
            parser.error("No such tag: %s" % tag)
        targets = session.getBuildTargets(buildTagID=tag_info['id'])
        if not targets:
            warn("%(name)s is not a build tag for any target" % tag_info)
            targets = session.getBuildTargets(destTagID=tag_info['id'])
            if targets:
                maybe = {}.fromkeys([t['build_tag_name'] for t in targets])
                maybe = sorted(maybe.keys())
                warn("Suggested tags: %s" % ', '.join(maybe))
            error()
        tag_id = tag_info['id']

    for nvr in builds:
        data = session.getLatestBuilds(tag_id, package=nvr["name"])
        if len(data) == 0:
            warn("Package %s is not in tag %s" % (nvr["name"], tag))
        else:
            present_nvr = [x["nvr"] for x in data][0]
            expected_nvr = '%(name)s-%(version)s-%(release)s' % nvr
            if present_nvr != expected_nvr:
                warn("nvr %s is not current in tag %s\n  latest build in %s is %s" %
                     (expected_nvr, tag, tag, present_nvr))

    last_repo = None
    repo = session.getRepo(tag_id)

    while True:
        if builds and repo and repo != last_repo:
            if koji.util.checkForBuilds(session, tag_id, builds, repo['create_event'],
                                        latest=True):
                if not suboptions.quiet:
                    print("Successfully waited %s for %s to appear in the %s repo" %
                          (koji.util.duration(start), koji.util.printList(suboptions.builds), tag))
                return

        if (time.time() - start) >= (suboptions.timeout * 60.0):
            if not suboptions.quiet:
                if builds:
                    error("Unsuccessfully waited %s for %s to appear in the %s repo" %
                          (koji.util.duration(start), koji.util.printList(suboptions.builds), tag))
                else:
                    error("Unsuccessfully waited %s for a new %s repo" %
                          (koji.util.duration(start), tag))
            error()

        time.sleep(options.poll_interval)
        last_repo = repo
        repo = session.getRepo(tag_id)

        if not builds:
            if repo != last_repo:
                if not suboptions.quiet:
                    print("Successfully waited %s for a new %s repo" %
                          (koji.util.duration(start), tag))
                return
