from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    arg_filter,
    get_usage_str
)


def handle_edit_user(goptions, session, args):
    "[admin] Alter user information"
    usage = "usage: %prog edit-user <username> [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--rename", help="Rename the user")
    parser.add_option("--edit-krb", action="append", default=[], metavar="OLD=NEW",
                      help="Change kerberos principal of the user")
    parser.add_option("--add-krb", action="append", default=[], metavar="KRB",
                      help="Add kerberos principal of the user")
    parser.add_option("--remove-krb", action="append", default=[], metavar="KRB",
                      help="Remove kerberos principal of the user")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("You must specify the username of the user to edit")
    elif len(args) > 1:
        parser.error("This command only accepts one argument (username)")
    activate_session(session, goptions)
    user = args[0]
    princ_mappings = []
    for p in options.edit_krb:
        old, new = p.split('=', 1)
        princ_mappings.append({'old': arg_filter(old), 'new': arg_filter(new)})
    for a in options.add_krb:
        princ_mappings.append({'old': None, 'new': arg_filter(a)})
    for r in options.remove_krb:
        princ_mappings.append({'old': arg_filter(r), 'new': None})
    session.editUser(user, options.rename, princ_mappings)
