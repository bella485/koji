from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_list_permissions(goptions, session, args):
    "[info] List user permissions"
    usage = "usage: %prog list-permissions [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--user", help="List permissions for the given user")
    parser.add_option("--mine", action="store_true", help="List your permissions")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the header information")
    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error("This command takes no arguments")
    activate_session(session, goptions)
    perms = []
    if options.user:
        user = session.getUser(options.user)
        if not user:
            error("No such user: %s" % options.user)
        for p in session.getUserPerms(user['id']):
            perms.append({'name': p})
    elif options.mine:
        for p in session.getPerms():
            perms.append({'name': p})
    else:
        for p in session.getAllPerms():
            perms.append({'name': p['name'], 'description': p['description']})
    if perms:
        longest_perm = max([len(perm['name']) for perm in perms])
        perms = sorted(perms, key=lambda x: x['name'])
    else:
        longest_perm = 8
    if longest_perm < len('Permission name   '):
        longest_perm = len('Permission name   ')
    if not options.quiet:
        hdr = '{permname:<{longest_perm}}'
        hdr = hdr.format(longest_perm=longest_perm, permname='Permission name')
        if perms and perms[0].get('description'):
            hdr += "   Description".ljust(53)
        print(hdr)
        print(len(hdr) * '-')
    for perm in perms:
        line = '{permname:<{longest_perm}}'
        line = line.format(longest_perm=longest_perm, permname=perm['name'])
        if perm.get('description'):
            line += "   %s" % perm['description']
        print(line)


