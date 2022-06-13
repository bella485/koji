from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_revoke_permission(goptions, session, args):
    "[admin] Revoke a permission from a user"
    usage = "usage: %prog revoke-permission <permission> <user> [<user> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a permission and at least one user")
    activate_session(session, goptions)
    perm = args[0]
    names = args[1:]
    users = []
    for n in names:
        user = session.getUser(n)
        if user is None:
            parser.error("No such user: %s" % n)
        users.append(user)
    for user in users:
        session.revokePermission(user['name'], perm)
