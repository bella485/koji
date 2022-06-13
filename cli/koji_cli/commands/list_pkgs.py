from __future__ import absolute_import, division

import time
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str
)


def anon_handle_list_pkgs(goptions, session, args):
    "[info] Print the package listing for tag or for owner"
    usage = "usage: %prog list-pkgs [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--owner", help="Specify owner")
    parser.add_option("--tag", help="Specify tag")
    parser.add_option("--package", help="Specify package")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print header information")
    parser.add_option("--noinherit", action="store_true", help="Don't follow inheritance")
    parser.add_option("--show-blocked", action="store_true", help="Show blocked packages")
    parser.add_option("--show-dups", action="store_true", help="Show superseded owners")
    parser.add_option("--event", type='int', metavar="EVENT#", help="query at event")
    parser.add_option("--ts", type='int', metavar="TIMESTAMP",
                      help="query at last event before timestamp")
    parser.add_option("--repo", type='int', metavar="REPO#", help="query at event for a repo")
    (options, args) = parser.parse_args(args)
    if len(args) != 0:
        parser.error("This command takes no arguments")
    ensure_connection(session, goptions)
    opts = {}
    if options.owner:
        user = session.getUser(options.owner)
        if user is None:
            parser.error("No such user: %s" % options.owner)
        opts['userID'] = user['id']
    if options.tag:
        tag = session.getTag(options.tag)
        if tag is None:
            parser.error("No such tag: %s" % options.tag)
        opts['tagID'] = tag['id']
    if options.package:
        opts['pkgID'] = options.package
    allpkgs = False
    if not opts:
        # no limiting clauses were specified
        allpkgs = True
    opts['inherited'] = not options.noinherit
    # hiding dups only makes sense if we're querying a tag
    if options.tag:
        opts['with_dups'] = options.show_dups
    else:
        opts['with_dups'] = True
    event = koji.util.eventFromOpts(session, options)
    if event:
        opts['event'] = event['id']
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        print("Querying at event %(id)i (%(timestr)s)" % event)

    if not opts.get('tagID') and not opts.get('userID') and \
       not opts.get('pkgID'):
        if opts.get('event'):
            parser.error("--event and --ts makes sense only with --tag,"
                         " --owner or --package")
        if options.show_blocked:
            parser.error("--show-blocked makes sense only with --tag,"
                         " --owner or --package")
    if options.show_blocked:
        opts['with_blocked'] = options.show_blocked

    try:
        data = session.listPackages(**opts)
    except koji.ParameterError:
        del opts['with_blocked']
        data = session.listPackages(**opts)

    if not data:
        error("(no matching packages)")
    if not options.quiet:
        if allpkgs:
            print("Package")
            print('-' * 23)
        else:
            print("%-23s %-23s %-16s %-15s" % ('Package', 'Tag', 'Extra Arches', 'Owner'))
            print("%s %s %s %s" % ('-' * 23, '-' * 23, '-' * 16, '-' * 15))
    for pkg in data:
        if allpkgs:
            print(pkg['package_name'])
        else:
            if not options.show_blocked and pkg.get('blocked', False):
                continue
            if 'tag_id' in pkg:
                if pkg['extra_arches'] is None:
                    pkg['extra_arches'] = ""
                fmt = "%(package_name)-23s %(tag_name)-23s %(extra_arches)-16s %(owner_name)-15s"
                if pkg.get('blocked', False):
                    fmt += " [BLOCKED]"
            else:
                fmt = "%(package_name)s"
            print(fmt % pkg)
