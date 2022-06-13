from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_pkg(goptions, session, args):
    "[admin] Add a package to the listing for tag"
    usage = "usage: %prog add-pkg [options] --owner <owner> <tag> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action='store_true', help="Override blocks if necessary")
    parser.add_option("--owner", help="Specify owner")
    parser.add_option("--extra-arches", help="Specify extra arches")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a tag and at least one package")
    if not options.owner:
        parser.error("Please specify an owner for the package(s)")
    if not session.getUser(options.owner):
        error("No such user: %s" % options.owner)
    activate_session(session, goptions)
    tag = args[0]
    opts = {}
    opts['force'] = options.force
    opts['block'] = False
    # check if list of packages exists for that tag already
    dsttag = session.getTag(tag)
    if dsttag is None:
        error("No such tag: %s" % tag)
    try:
        pkglist = session.listPackages(tagID=dsttag['id'], with_owners=False)
    except koji.ParameterError:
        # performance option added in 1.25
        pkglist = session.listPackages(tagID=dsttag['id'])
    pkglist = dict([(p['package_name'], p['package_id']) for p in pkglist])
    to_add = []
    for package in args[1:]:
        package_id = pkglist.get(package, None)
        if package_id is not None:
            print("Package %s already exists in tag %s" % (package, tag))
            continue
        to_add.append(package)
    if options.extra_arches:
        opts['extra_arches'] = koji.parse_arches(options.extra_arches)

    # add the packages
    print("Adding %i packages to tag %s" % (len(to_add), dsttag['name']))
    session.multicall = True
    for package in to_add:
        session.packageListAdd(tag, package, options.owner, **opts)
    session.multiCall(strict=True)


