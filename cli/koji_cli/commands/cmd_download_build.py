from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip

import koji

from koji_cli.lib import (
    download_archive,
    download_rpm,
    ensure_connection,
    error,
    get_usage_str,
    warn
)


def anon_handle_download_build(options, session, args):
    "[download] Download a built package"
    usage = "usage: %prog download-build [options] <n-v-r | build_id | package>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arch", "-a", dest="arches", metavar="ARCH", action="append", default=[],
                      help="Only download packages for this arch (may be used multiple times)")
    parser.add_option("--type",
                      help="Download archives of the given type, rather than rpms "
                           "(maven, win, image, remote-sources)")
    parser.add_option("--latestfrom", dest="latestfrom",
                      help="Download the latest build from this tag")
    parser.add_option("--debuginfo", action="store_true", help="Also download -debuginfo rpms")
    parser.add_option("--task-id", action="store_true", help="Interperet id as a task id")
    parser.add_option("--rpm", action="store_true", help="Download the given rpm")
    parser.add_option("--key", help="Download rpms signed with the given key")
    parser.add_option("--topurl", metavar="URL", default=options.topurl,
                      help="URL under which Koji files are accessible")
    parser.add_option("--noprogress", action="store_true", help="Do not display progress meter")
    parser.add_option("-q", "--quiet", action="store_true",
                      help="Suppress output", default=options.quiet)
    (suboptions, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify a package N-V-R or build ID")
    elif len(args) > 1:
        parser.error("Only a single package N-V-R or build ID may be specified")

    ensure_connection(session, options)
    build = args[0]

    if build.isdigit():
        if suboptions.latestfrom:
            parser.error("--latestfrom not compatible with build IDs, specify a package name.")
        build = int(build)
        if suboptions.task_id:
            builds = session.listBuilds(taskID=build)
            if not builds:
                error("No associated builds for task %s" % build)
            build = builds[0]['build_id']

    if suboptions.latestfrom:
        # We want the latest build, not a specific build
        try:
            builds = session.listTagged(suboptions.latestfrom, latest=True, package=build,
                                        type=suboptions.type)
        except koji.GenericError as data:
            error("Error finding latest build: %s" % data)
        if not builds:
            error("%s has no builds of %s" % (suboptions.latestfrom, build))
        info = builds[0]
    elif suboptions.rpm:
        rpminfo = session.getRPM(build)
        if rpminfo is None:
            error("No such rpm: %s" % build)
        info = session.getBuild(rpminfo['build_id'])
    else:
        # if we're given an rpm name without --rpm, download the containing build
        try:
            koji.parse_NVRA(build)
            rpminfo = session.getRPM(build)
            build = rpminfo['build_id']
        except Exception:
            pass
        info = session.getBuild(build)

    if info is None:
        error("No such build: %s" % build)

    if not suboptions.topurl:
        error("You must specify --topurl to download files")

    archives = []
    rpms = []
    if suboptions.type:
        archives = session.listArchives(buildID=info['id'], type=suboptions.type)
        if not archives:
            error("No %s archives available for %s" % (suboptions.type, koji.buildLabel(info)))
    else:
        arches = suboptions.arches
        if len(arches) == 0:
            arches = None
        if suboptions.rpm:
            all_rpms = [rpminfo]
        else:
            all_rpms = session.listRPMs(buildID=info['id'], arches=arches)
        if not all_rpms:
            if arches:
                error("No %s packages available for %s" %
                      (" or ".join(arches), koji.buildLabel(info)))
            else:
                error("No packages available for %s" % koji.buildLabel(info))
        for rpm in all_rpms:
            if not suboptions.debuginfo and koji.is_debuginfo(rpm['name']):
                continue
            rpms.append(rpm)

    if suboptions.key:
        with session.multicall() as m:
            results = [m.queryRPMSigs(rpm_id=r['id'], sigkey=suboptions.key) for r in rpms]
        rpm_keys = [x.result for x in results]
        for rpm, rpm_key in list(zip(rpms, rpm_keys)):
            if not rpm_key:
                nvra = "%(nvr)s-%(arch)s.rpm" % rpm
                warn("No such sigkey %s for rpm %s" % (suboptions.key, nvra))
                rpms.remove(rpm)

    # run the download
    for rpm in rpms:
        download_rpm(info, rpm, suboptions.topurl, sigkey=suboptions.key,
                     quiet=suboptions.quiet, noprogress=suboptions.noprogress)
    for archive in archives:
        download_archive(info, archive, suboptions.topurl,
                         quiet=suboptions.quiet, noprogress=suboptions.noprogress)


