from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_add_user(goptions, session, args):
    "[admin] Add a user"
    usage = "usage: %prog add-user <username> [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--principal", help="The Kerberos principal for this user")
    parser.add_option("--disable", help="Prohibit logins by this user", action="store_true")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("You must specify the username of the user to add")
    elif len(args) > 1:
        parser.error("This command only accepts one argument (username)")
    username = args[0]
    if options.disable:
        status = koji.USER_STATUS['BLOCKED']
    else:
        status = koji.USER_STATUS['NORMAL']
    activate_session(session, goptions)
    user_id = session.createUser(username, status=status, krb_principal=options.principal)
    print("Added user %s (%i)" % (username, user_id))


