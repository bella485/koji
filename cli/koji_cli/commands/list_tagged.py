from __future__ import absolute_import, division

import os
import time
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_tagged(goptions, session, args):
    "[info] List the builds or rpms in a tag"
    usage = "usage: %prog list-tagged [options] <tag> [<package>]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arch", action="append", default=[], help="List rpms for this arch")
    parser.add_option("--rpms", action="store_true", help="Show rpms instead of builds")
    parser.add_option("--inherit", action="store_true", help="Follow inheritance")
    parser.add_option("--latest", action="store_true", help="Only show the latest builds/rpms")
    parser.add_option("--latest-n", type='int', metavar="N",
                      help="Only show the latest N builds/rpms")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the header information")
    parser.add_option("--paths", action="store_true", help="Show the file paths")
    parser.add_option("--sigs", action="store_true", help="Show signatures")
    parser.add_option("--type",
                      help="Show builds of the given type only. "
                           "Currently supported types: maven, win, image")
    parser.add_option("--event", type='int', metavar="EVENT#", help="query at event")
    parser.add_option("--ts", type='int', metavar="TIMESTAMP",
                      help="query at last event before timestamp")
    parser.add_option("--repo", type='int', metavar="REPO#", help="query at event for a repo")
    (options, args) = parser.parse_args(args)
    if len(args) == 0:
        parser.error("A tag name must be specified")
    elif len(args) > 2:
        parser.error("Only one package name may be specified")
    ensure_connection(session, goptions)
    pathinfo = koji.PathInfo()
    package = None
    if len(args) > 1:
        package = args[1]
    tag = args[0]
    opts = {}
    for key in ('latest', 'inherit'):
        opts[key] = getattr(options, key)
    if options.latest_n is not None:
        opts['latest'] = options.latest_n
    if package:
        opts['package'] = package
    if options.arch:
        options.rpms = True
        opts['arch'] = options.arch
    if options.sigs:
        opts['rpmsigs'] = True
        options.rpms = True
    if options.type:
        opts['type'] = options.type
    event = koji.util.eventFromOpts(session, options)
    event_id = None
    if event:
        opts['event'] = event['id']
        event_id = event['id']
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        if not options.quiet:
            print("Querying at event %(id)i (%(timestr)s)" % event)

    # check if tag exist(s|ed)
    taginfo = session.getTag(tag, event=event_id)
    if not taginfo:
        parser.error("No such tag: %s" % tag)

    if options.rpms:
        rpms, builds = session.listTaggedRPMS(tag, **opts)
        data = rpms
        if options.paths:
            build_idx = dict([(b['id'], b) for b in builds])
            for rinfo in data:
                build = build_idx[rinfo['build_id']]
                builddir = pathinfo.build(build)
                if options.sigs:
                    sigkey = rinfo['sigkey']
                    signedpath = os.path.join(builddir, pathinfo.signed(rinfo, sigkey))
                    if os.path.exists(signedpath):
                        rinfo['path'] = signedpath
                else:
                    rinfo['path'] = os.path.join(builddir, pathinfo.rpm(rinfo))
            fmt = "%(path)s"
            data = [x for x in data if 'path' in x]
        else:
            fmt = "%(name)s-%(version)s-%(release)s.%(arch)s"
            if options.sigs:
                fmt = "%(sigkey)s " + fmt
    else:
        data = session.listTagged(tag, **opts)
        if options.paths:
            if options.type == 'maven':
                for x in data:
                    x['path'] = pathinfo.mavenbuild(x)
                fmt = "%(path)-40s  %(tag_name)-20s  %(maven_group_id)-20s  " \
                      "%(maven_artifact_id)-20s  %(owner_name)s"
            else:
                for x in data:
                    x['path'] = pathinfo.build(x)
                fmt = "%(path)-40s  %(tag_name)-20s  %(owner_name)s"
        else:
            if options.type == 'maven':
                fmt = "%(nvr)-40s  %(tag_name)-20s  %(maven_group_id)-20s  " \
                      "%(maven_artifact_id)-20s  %(owner_name)s"
            else:
                fmt = "%(nvr)-40s  %(tag_name)-20s  %(owner_name)s"
        if not options.quiet:
            if options.type == 'maven':
                print("%-40s  %-20s  %-20s  %-20s  %s" %
                      ("Build", "Tag", "Group Id", "Artifact Id", "Built by"))
                print("%s  %s  %s  %s  %s" %
                      ("-" * 40, "-" * 20, "-" * 20, "-" * 20, "-" * 16))
            else:
                print("%-40s  %-20s  %s" % ("Build", "Tag", "Built by"))
                print("%s  %s  %s" % ("-" * 40, "-" * 20, "-" * 16))

    output = sorted([fmt % x for x in data])
    for line in output:
        print(line)
