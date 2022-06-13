from __future__ import absolute_import, division

import os
import time
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str,
    warn
)


def anon_handle_rpminfo(goptions, session, args):
    "[info] Print basic information about an RPM"
    usage = "usage: %prog rpminfo [options] <n-v-r.a> [<n-v-r.a> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--buildroots", action="store_true",
                      help="show buildroots the rpm was used in")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify an RPM")
    ensure_connection(session, goptions)
    error_hit = False
    for rpm in args:
        info = session.getRPM(rpm)
        if info is None:
            warn("No such rpm: %s\n" % rpm)
            error_hit = True
            continue
        if info['epoch'] is None:
            info['epoch'] = ""
        else:
            info['epoch'] = str(info['epoch']) + ":"
        if not info.get('external_repo_id', 0):
            buildinfo = session.getBuild(info['build_id'])
            buildinfo['name'] = buildinfo['package_name']
            buildinfo['arch'] = 'src'
            if buildinfo['epoch'] is None:
                buildinfo['epoch'] = ""
            else:
                buildinfo['epoch'] = str(buildinfo['epoch']) + ":"
        print("RPM: %(epoch)s%(name)s-%(version)s-%(release)s.%(arch)s [%(id)d]" % info)
        if info.get('external_repo_id'):
            repo = session.getExternalRepo(info['external_repo_id'])
            print("External Repository: %(name)s [%(id)i]" % repo)
            print("External Repository url: %(url)s" % repo)
        else:
            print("RPM Path: %s" %
                  os.path.join(koji.pathinfo.build(buildinfo), koji.pathinfo.rpm(info)))
            print("SRPM: %(epoch)s%(name)s-%(version)s-%(release)s [%(id)d]" % buildinfo)
            print("SRPM Path: %s" %
                  os.path.join(koji.pathinfo.build(buildinfo), koji.pathinfo.rpm(buildinfo)))
            print("Built: %s" % time.strftime('%a, %d %b %Y %H:%M:%S %Z',
                                              time.localtime(info['buildtime'])))
        print("SIGMD5: %(payloadhash)s" % info)
        print("Size: %(size)s" % info)
        if not info.get('external_repo_id', 0):
            headers = session.getRPMHeaders(rpmID=info['id'],
                                            headers=["license"])
            if 'license' in headers:
                print("License: %(license)s" % headers)
            print("Build ID: %(build_id)s" % info)
        if info['buildroot_id'] is None:
            print("No buildroot data available")
        else:
            br_info = session.getBuildroot(info['buildroot_id'])
            if br_info['br_type'] == koji.BR_TYPES['STANDARD']:
                print("Buildroot: %(id)i (tag %(tag_name)s, arch %(arch)s, repo %(repo_id)i)" %
                      br_info)
                print("Build Host: %(host_name)s" % br_info)
                print("Build Task: %(task_id)i" % br_info)
            else:
                print("Content generator: %(cg_name)s" % br_info)
                print("Buildroot: %(id)i" % br_info)
                print("Build Host OS: %(host_os)s (%(host_arch)s)" % br_info)
        if info.get('extra'):
            print("Extra: %(extra)r" % info)
        if options.buildroots:
            br_list = session.listBuildroots(rpmID=info['id'], queryOpts={'order': 'buildroot.id'})
            print("Used in %i buildroots:" % len(br_list))
            if len(br_list):
                print("  %8s %-28s %-8s %-29s" % ('id', 'build tag', 'arch', 'build host'))
                print("  %s %s %s %s" % ('-' * 8, '-' * 28, '-' * 8, '-' * 29))
            for br_info in br_list:
                print("  %(id)8i %(tag_name)-28s %(arch)-8s %(host_name)-29s" % br_info)
    if error_hit:
        error()
