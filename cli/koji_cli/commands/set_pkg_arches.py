from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_set_pkg_arches(goptions, session, args):
    "[admin] Set the list of extra arches for a package"
    usage = "usage: %prog set-pkg-arches [options] <arches> <tag> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action='store_true', help="Force operation")
    (options, args) = parser.parse_args(args)
    if len(args) < 3:
        parser.error("Please specify an archlist, a tag, and at least one package")
    activate_session(session, goptions)
    arches = koji.parse_arches(args[0])
    tag = args[1]
    with session.multicall(strict=True) as m:
        for package in args[2:]:
            m.packageListSetArches(tag, package, arches, force=options.force)
