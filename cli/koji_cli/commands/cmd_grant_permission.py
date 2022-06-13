from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_grant_permission(goptions, session, args):
    "[admin] Grant a permission to a user"
    usage = "usage: %prog grant-permission [options] <permission> <user> [<user> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--new", action="store_true",
                      help="Create this permission if the permission does not exist")
    parser.add_option("--description",
                      help="Add description about new permission")
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
    kwargs = {}
    if options.new:
        kwargs['create'] = True
        if options.description:
            kwargs['description'] = options.description
    if options.description and not options.new:
        parser.error("Option new must be specified with option description.")
    for user in users:
        session.grantPermission(user['name'], perm, **kwargs)


