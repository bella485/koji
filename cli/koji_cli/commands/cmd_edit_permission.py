from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_edit_permission(goptions, session, args):
    "[admin] Edit a permission description"
    usage = "usage: %prog edit-permission <permission> <description>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a permission and a description")
    activate_session(session, goptions)
    perm = args[0]
    description = args[1]
    session.editPermission(perm, description)


