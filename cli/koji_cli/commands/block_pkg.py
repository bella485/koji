from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_block_pkg(goptions, session, args):
    "[admin] Block a package in the listing for tag"
    usage = "usage: %prog block-pkg [options] <tag> <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action='store_true', default=False,
                      help="Override blocks and owner if necessary")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a tag and at least one package")
    activate_session(session, goptions)
    tag = args[0]
    # check if list of packages exists for that tag already
    dsttag = session.getTag(tag)
    if dsttag is None:
        error("No such tag: %s" % tag)
    try:
        pkglist = session.listPackages(tagID=dsttag['id'], inherited=True, with_owners=False)
    except koji.ParameterError:
        # performance option added in 1.25
        pkglist = session.listPackages(tagID=dsttag['id'], inherited=True)
    pkglist = dict([(p['package_name'], p['package_id']) for p in pkglist])
    ret = 0
    for package in args[1:]:
        package_id = pkglist.get(package, None)
        if package_id is None:
            warn("Package %s doesn't exist in tag %s" % (package, tag))
            ret = 1
    if ret:
        error(code=ret)
    session.multicall = True
    for package in args[1:]:
        # force is not supported on older hub, so use it only explicitly
        # https://pagure.io/koji/issue/1388
        if options.force:
            session.packageListBlock(tag, package, force=options.force)
        else:
            session.packageListBlock(tag, package)
    session.multiCall(strict=True)
