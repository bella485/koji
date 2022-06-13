from __future__ import absolute_import, division

import itertools
from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_untag_build(goptions, session, args):
    "[bind] Remove a tag from one or more builds"
    usage = "usage: %prog untag-build [options] <tag> <pkg> [<pkg> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--all", action="store_true",
                      help="untag all versions of the package in this tag, pkg is package name")
    parser.add_option("--non-latest", action="store_true",
                      help="untag all versions of the package in this tag except the latest, "
                           "pkg is package name")
    parser.add_option("-n", "--test", action="store_true", help="test mode")
    parser.add_option("-v", "--verbose", action="store_true", help="print details")
    parser.add_option("--force", action="store_true", help="force operation")
    (options, args) = parser.parse_args(args)
    if options.non_latest and options.force:
        if len(args) < 1:
            parser.error("Please specify a tag")
    elif len(args) < 2:
        parser.error(
            "This command takes at least two arguments: a tag name/ID and one or more package "
            "n-v-r's or package names")
    activate_session(session, goptions)
    tag = session.getTag(args[0])
    if not tag:
        parser.error("No such tag: %s" % args[0])
    if options.all:
        result = []
        with session.multicall() as m:
            result.extend([m.queryHistory(tag=args[0], package=pkg, active=True)
                           for pkg in args[1:]])
        builds = []
        for r in result:
            builds.extend(r.result['tag_listing'])
    elif options.non_latest:
        if options.force and len(args) == 1:
            tagged = session.queryHistory(tag=args[0], active=True)['tag_listing']
            tagged = sorted(tagged, key=lambda k: (k['create_event']), reverse=True)
        else:
            result = []
            with session.multicall() as m:
                result.extend([m.queryHistory(tag=args[0], package=pkg, active=True)
                               for pkg in args[1:]])
            tagged = []
            for r in result:
                tagged.extend(r.result['tag_listing'])
            tagged = sorted(tagged, key=lambda k: (k['create_event']), reverse=True)
        # listTagged orders entries latest first
        seen_pkg = {}
        builds = []
        for binfo in tagged:
            if binfo['name'] not in seen_pkg:
                # latest for this package
                nvr = '%s-%s-%s' % (binfo['name'], binfo['version'], binfo['release'])
                if options.verbose:
                    print("Leaving latest build for package %s: %s" % (binfo['name'], nvr))
            else:
                builds.append(binfo)
            seen_pkg[binfo['name']] = 1
    else:
        # find all pkg's builds in tag
        pkgs = set([koji.parse_NVR(nvr)['name'] for nvr in args[1:]])
        result = []
        with session.multicall() as m:
            result.extend([m.queryHistory(tag=args[0], pkg=pkg, active=True) for pkg in pkgs])
        tagged = []
        for r in result:
            tagged.append(r.result['tag_listing'])
        # flatten
        tagged = list(itertools.chain(*[t for t in tagged]))
        idx = dict([('%s-%s-%s' % (b['name'], b['version'], b['release']), b) for b in tagged])

        # check exact builds
        builds = []
        for nvr in args[1:]:
            binfo = idx.get(nvr)
            if binfo:
                builds.append(binfo)
            else:
                # not in tag, see if it even exists
                binfo = session.getBuild(nvr)
                if not binfo:
                    warn("No such build: %s" % nvr)
                else:
                    warn("Build %s not in tag %s" % (nvr, tag['name']))
                if not options.force:
                    error()
    builds.reverse()
    with session.multicall(strict=True) as m:
        for binfo in builds:
            build_nvr = '%s-%s-%s' % (binfo['name'], binfo['version'], binfo['release'])
            if options.test:
                print("would have untagged %s" % build_nvr)
            else:
                if options.verbose:
                    print("untagging %s" % build_nvr)
                m.untagBuild(tag['name'], build_nvr, force=options.force)
