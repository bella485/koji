from __future__ import absolute_import, division

from optparse import OptionParser

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_add_group_pkg(goptions, session, args):
    "[admin] Add a package to a group's package listing"
    usage = "usage: %prog add-group-pkg [options] <tag> <group> <pkg> [<pkg> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 3:
        parser.error("You must specify a tag name, group name, and one or more package names")
    tag = args[0]
    group = args[1]
    activate_session(session, goptions)
    for pkg in args[2:]:
        session.groupPackageListAdd(tag, group, pkg)
