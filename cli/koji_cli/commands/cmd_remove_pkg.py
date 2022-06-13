from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_remove_pkg(goptions, session, args):
    "[admin] Remove a package from the listing for tag"
    usage = "usage: %prog remove-pkg [options] <tag> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action='store_true', help="Override blocks if necessary")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a tag and at least one package")
    activate_session(session, goptions)
    tag = args[0]
    opts = {}
    opts['force'] = options.force
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
    ret = 0
    for package in args[1:]:
        package_id = pkglist.get(package, None)
        if package_id is None:
            warn("Package %s is not in tag %s" % (package, tag))
            ret = 1
    if ret:
        error(code=ret)
    session.multicall = True
    for package in args[1:]:
        session.packageListRemove(tag, package, **opts)
    session.multiCall(strict=True)


