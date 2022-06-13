from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_disable_user(goptions, session, args):
    "[admin] Disable logins by a user"
    usage = "usage: %prog disable-user <username>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("You must specify the username of the user to disable")
    elif len(args) > 1:
        parser.error("This command only accepts one argument (username)")
    username = args[0]
    activate_session(session, goptions)
    session.disableUser(username)


