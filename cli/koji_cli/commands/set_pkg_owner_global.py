from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_set_pkg_owner_global(goptions, session, args):
    "[admin] Set the owner for a package globally"
    usage = "usage: %prog set-pkg-owner-global [options] <owner> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--verbose", action='store_true', help="List changes")
    parser.add_option("--test", action='store_true', help="Test mode")
    parser.add_option("--old-user", "--from", action="store",
                      help="Only change ownership for packages belonging to this user")
    (options, args) = parser.parse_args(args)
    if options.old_user:
        if len(args) < 1:
            parser.error("Please specify an owner")
    elif len(args) < 2:
        parser.error("Please specify an owner and at least one package")
    activate_session(session, goptions)
    owner = args[0]
    packages = args[1:]
    user = session.getUser(owner)
    if not user:
        error("No such user: %s" % owner)
    opts = {'with_dups': True}
    old_user = None
    if options.old_user:
        old_user = session.getUser(options.old_user)
        if not old_user:
            error("No such user: %s" % options.old_user)
        opts['userID'] = old_user['id']
    to_change = []
    for package in packages:
        entries = session.listPackages(pkgID=package, **opts)
        if not entries:
            print("No data for package %s" % package)
            continue
        to_change.extend(entries)
    if not packages and options.old_user:
        entries = session.listPackages(**opts)
        if not entries:
            error("No data for user %s" % old_user['name'])
        to_change.extend(entries)
    for entry in to_change:
        if user['id'] == entry['owner_id']:
            if options.verbose:
                print("Preserving owner=%s for package %s in tag %s"
                      % (user['name'], package, entry['tag_name']))
        else:
            if options.test:
                print("Would have changed owner for %s in tag %s: %s -> %s"
                      % (entry['package_name'], entry['tag_name'], entry['owner_name'],
                         user['name']))
                continue
            if options.verbose:
                print("Changing owner for %s in tag %s: %s -> %s"
                      % (entry['package_name'], entry['tag_name'], entry['owner_name'],
                         user['name']))
            session.packageListSetOwner(entry['tag_id'], entry['package_name'], user['id'])
