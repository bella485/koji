from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_unblock_pkg(goptions, session, args):
    "[admin] Unblock a package in the listing for tag"
    usage = "usage: %prog unblock-pkg [options] <tag> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a tag and at least one package")
    activate_session(session, goptions)
    tag = args[0]
    with session.multicall(strict=True) as m:
        for package in args[1:]:
            m.packageListUnblock(tag, package)


