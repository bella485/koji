from __future__ import absolute_import, division

import itertools
import sys
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str,
)


def anon_handle_list_tags(goptions, session, args):
    "[info] Print the list of tags"
    usage = "usage: %prog list-tags [options] [pattern]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--show-id", action="store_true", help="Show tag ids")
    parser.add_option("--verbose", action="store_true", help="Show more information")
    parser.add_option("--unlocked", action="store_true", help="Only show unlocked tags")
    parser.add_option("--build", help="Show tags associated with a build")
    parser.add_option("--package", help="Show tags associated with a package")
    (options, patterns) = parser.parse_args(args)
    ensure_connection(session, goptions)

    pkginfo = {}
    buildinfo = {}

    if options.package:
        pkginfo = session.getPackage(options.package)
        if not pkginfo:
            parser.error("No such package: %s" % options.package)

    if options.build:
        buildinfo = session.getBuild(options.build)
        if not buildinfo:
            parser.error("No such build: %s" % options.build)

    if not patterns:
        # list everything if no pattern is supplied
        tags = session.listTags(build=buildinfo.get('id', None),
                                package=pkginfo.get('id', None))
    else:
        # The hub may not support the pattern option. We try with that first
        # and fall back to the old way.
        fallback = False
        try:
            tags = []
            with session.multicall(strict=True) as m:
                for pattern in patterns:
                    tags.append(m.listTags(build=buildinfo.get('id', None),
                                           package=pkginfo.get('id', None),
                                           pattern=pattern))
            tags = list(itertools.chain(*[t.result for t in tags]))
        except koji.ParameterError:
            fallback = True
        if fallback:
            # without the pattern option, we have to filter client side
            tags = session.listTags(buildinfo.get('id', None), pkginfo.get('id', None))
            tags = [t for t in tags if koji.util.multi_fnmatch(t['name'], patterns)]

    tags.sort(key=lambda x: x['name'])
    # if options.verbose:
    #    fmt = "%(name)s [%(id)i] %(perm)s %(locked)s %(arches)s"
    if options.show_id:
        fmt = "%(name)s [%(id)i]"
    else:
        fmt = "%(name)s"
    for tag in tags:
        if options.unlocked:
            if tag['locked'] or tag['perm']:
                continue
        if not options.verbose:
            print(fmt % tag)
        else:
            sys.stdout.write(fmt % tag)
            if tag['locked']:
                sys.stdout.write(' [LOCKED]')
            if tag['perm']:
                sys.stdout.write(' [%(perm)s perm required]' % tag)
            print('')
