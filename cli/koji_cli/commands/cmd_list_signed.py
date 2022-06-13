from __future__ import absolute_import, division

import os
from optparse import SUPPRESS_HELP, OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_list_signed(goptions, session, args):
    "[admin] List signed copies of rpms"
    usage = "usage: %prog list-signed [options]"
    description = "You must have local access to Koji's topdir filesystem."
    parser = OptionParser(usage=get_usage_str(usage), description=description)
    # Don't use local debug option, this one stays here for backward compatibility
    # https://pagure.io/koji/issue/2084
    parser.add_option("--debug", action="store_true", default=goptions.debug, help=SUPPRESS_HELP)
    parser.add_option("--key", help="Only list RPMs signed with this key")
    parser.add_option("--build", help="Only list RPMs from this build")
    parser.add_option("--rpm", help="Only list signed copies for this RPM")
    parser.add_option("--tag", help="Only list RPMs within this tag")
    (options, args) = parser.parse_args(args)
    if not options.build and not options.tag and not options.rpm:
        parser.error("At least one from --build, --rpm, --tag needs to be specified.")
    activate_session(session, goptions)
    qopts = {}
    build_idx = {}
    rpm_idx = {}
    if options.key:
        qopts['sigkey'] = options.key

    sigs = []
    if options.rpm:
        rpm_info = options.rpm
        try:
            rpm_info = int(rpm_info)
        except ValueError:
            pass
        rinfo = session.getRPM(rpm_info, strict=True)
        rpm_idx[rinfo['id']] = rinfo
        if rinfo.get('external_repo_id'):
            parser.error("External rpm: %(name)s-%(version)s-%(release)s.%(arch)s@"
                         "%(external_repo_name)s" % rinfo)
        sigs += session.queryRPMSigs(rpm_id=rinfo['id'], **qopts)
    if options.build:
        build = options.build
        try:
            build = int(build)
        except ValueError:
            pass
        binfo = session.getBuild(build, strict=True)
        build_idx[binfo['id']] = binfo
        rpms = session.listRPMs(buildID=binfo['id'])
        for rinfo in rpms:
            rpm_idx[rinfo['id']] = rinfo
            sigs += session.queryRPMSigs(rpm_id=rinfo['id'], **qopts)
    if options.tag:
        tag = options.tag
        try:
            tag = int(tag)
        except ValueError:
            pass
        rpms, builds = session.listTaggedRPMS(tag, inherit=False, latest=False)
        tagged = {}
        for binfo in builds:
            build_idx.setdefault(binfo['id'], binfo)
        results = []
        # use batched multicall as there could be potentially a lot of results
        # so we don't exhaust server resources
        with session.multicall(batch=5000) as m:
            for rinfo in rpms:
                rpm_idx.setdefault(rinfo['id'], rinfo)
                tagged[rinfo['id']] = 1
                results.append(m.queryRPMSigs(rpm_id=rinfo['id']), **qopts)
        sigs += [x.result[0] for x in results]

    # Now figure out which sig entries actually have live copies
    for sig in sigs:
        rpm_id = sig['rpm_id']
        sigkey = sig['sigkey']
        if options.tag:
            if tagged.get(rpm_id) is None:
                continue
        rinfo = rpm_idx.get(rpm_id)
        if not rinfo:
            rinfo = session.getRPM(rpm_id)
            rpm_idx[rinfo['id']] = rinfo
        binfo = build_idx.get(rinfo['build_id'])
        if not binfo:
            binfo = session.getBuild(rinfo['build_id'])
            build_idx[binfo['id']] = binfo
        binfo['name'] = binfo['package_name']
        builddir = koji.pathinfo.build(binfo)
        signedpath = "%s/%s" % (builddir, koji.pathinfo.signed(rinfo, sigkey))
        if not os.path.exists(signedpath):
            if goptions.debug:
                print("No copy: %s" % signedpath)
            continue
        print(signedpath)


