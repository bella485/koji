from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_set_pkg_owner(goptions, session, args):
    "[admin] Set the owner for a package"
    usage = "usage: %prog set-pkg-owner [options] <owner> <tag> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action='store_true', help="Force operation")
    (options, args) = parser.parse_args(args)
    if len(args) < 3:
        parser.error("Please specify an owner, a tag, and at least one package")
    activate_session(session, goptions)
    owner = args[0]
    tag = args[1]
    with session.multicall(strict=True) as m:
        for package in args[2:]:
            m.packageListSetOwner(tag, package, owner, force=options.force)
