from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip

import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_write_signed_rpm(goptions, session, args):
    "[admin] Write signed RPMs to disk"
    usage = "usage: %prog write-signed-rpm [options] <signature-key> <n-v-r> [<n-v-r> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--all", action="store_true", help="Write out all RPMs signed with this key")
    parser.add_option("--buildid", help="Specify a build id rather than an n-v-r")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("A signature key must be specified")
    if len(args) < 2 and not (options.all or options.buildid):
        parser.error("At least one RPM must be specified")
    key = args.pop(0).lower()
    activate_session(session, goptions)
    if options.all:
        rpms = session.queryRPMSigs(sigkey=key)
        with session.multicall() as m:
            results = [m.getRPM(r['rpm_id']) for r in rpms]
        rpms = [x.result for x in results]
    elif options.buildid:
        rpms = session.listRPMs(int(options.buildid))
    else:
        nvrs = []
        rpms = []

        with session.multicall() as m:
            result = [m.getRPM(nvra, strict=False) for nvra in args]
        for rpm, nvra in zip(result, args):
            rpm = rpm.result
            if rpm:
                rpms.append(rpm)
            else:
                nvrs.append(nvra)

        # for historical reasons, we also accept nvrs
        with session.multicall() as m:
            result = [m.getBuild(nvr, strict=True) for nvr in nvrs]
        builds = []
        for nvr, build in zip(nvrs, result):
            try:
                builds.append(build.result['id'])
            except koji.GenericError:
                raise koji.GenericError("No such rpm or build: %s" % nvr)

        with session.multicall() as m:
            rpm_lists = [m.listRPMs(buildID=build_id) for build_id in builds]
        for rpm_list in rpm_lists:
            rpms.extend(rpm_list.result)

    with session.multicall(strict=True) as m:
        for i, rpminfo in enumerate(rpms):
            nvra = "%(name)s-%(version)s-%(release)s.%(arch)s" % rpminfo
            print("[%d/%d] %s" % (i + 1, len(rpms), nvra))
            m.writeSignedRPM(rpminfo['id'], key)


