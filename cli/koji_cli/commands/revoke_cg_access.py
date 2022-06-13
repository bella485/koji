from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_revoke_cg_access(goptions, session, args):
    "[admin] Remove a user from a content generator"
    usage = "usage: %prog revoke-cg-access <user> <content generator>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) != 2:
        parser.error("Please specify a user and content generator")
    activate_session(session, goptions)
    user = args[0]
    cg = args[1]
    uinfo = session.getUser(user)
    if uinfo is None:
        parser.error("No such user: %s" % user)
    session.revokeCGAccess(uinfo['name'], cg)
