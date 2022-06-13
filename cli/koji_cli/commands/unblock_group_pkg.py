from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_unblock_group_pkg(goptions, session, args):
    "[admin] Unblock a package from a group's package listing"
    usage = "usage: %prog unblock-group-pkg [options] <tag> <group> <pkg> [<pkg> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 3:
        parser.error("You must specify a tag name, group name, and one or more package names")
    tag = args[0]
    group = args[1]
    activate_session(session, goptions)
    for pkg in args[2:]:
        session.groupPackageListUnblock(tag, group, pkg)
