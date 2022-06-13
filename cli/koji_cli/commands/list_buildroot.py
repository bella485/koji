from __future__ import absolute_import, division

from optparse import SUPPRESS_HELP, OptionParser

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_buildroot(goptions, session, args):
    "[info] List the rpms used in or built in a buildroot"
    usage = "usage: %prog list-buildroot [options] <buildroot-id>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--paths", action="store_true", help=SUPPRESS_HELP)
    parser.add_option("--built", action="store_true", help="Show the built rpms")
    parser.add_option("--verbose", "-v", action="store_true", help="Show more information")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Incorrect number of arguments")
    ensure_connection(session, goptions)
    if options.paths:
        parser.error("--paths option is deprecated and will be removed in 1.30")
    buildrootID = int(args[0])
    opts = {}
    if options.built:
        opts['buildrootID'] = buildrootID
    else:
        opts['componentBuildrootID'] = buildrootID
    data = session.listRPMs(**opts)

    fmt = "%(nvr)s.%(arch)s"
    order = sorted([(fmt % x, x) for x in data])
    for nvra, rinfo in order:
        if options.verbose and rinfo.get('is_update'):
            print("%s [update]" % nvra)
        else:
            print(nvra)
