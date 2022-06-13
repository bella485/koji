from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    TimeOption,
    ensure_connection,
    get_usage_str,
    warn
)


def anon_handle_list_builds(goptions, session, args):
    "[info] Print the build listing"
    usage = "usage: %prog list-builds [options]"
    parser = OptionParser(usage=get_usage_str(usage), option_class=TimeOption)
    parser.add_option("--package", help="List builds for this package")
    parser.add_option("--buildid", help="List specific build from ID or nvr")
    parser.add_option("--before", type="time",
                      help="List builds built before this time, " + TimeOption.get_help())
    parser.add_option("--after", type="time",
                      help="List builds built after this time (same format as for --before")
    parser.add_option("--state", help="List builds in this state")
    parser.add_option("--task", help="List builds for this task")
    parser.add_option("--type", help="List builds of this type.")
    parser.add_option("--prefix", help="Only builds starting with this prefix")
    parser.add_option("--pattern", help="Only list builds matching this GLOB pattern")
    parser.add_option("--cg", help="Only list builds imported by matching content generator name")
    parser.add_option("--source", help="Only builds where the source field matches (glob pattern)")
    parser.add_option("--owner", help="List builds built by this owner")
    parser.add_option("--volume", help="List builds by volume ID")
    parser.add_option("-k", "--sort-key", action="append", metavar='FIELD',
                      default=[], help="Sort the list by the named field. Allowed sort keys: "
                                       "nvr, owner_name, state")
    parser.add_option("-r", "--reverse", action="store_true", default=False,
                      help="Print the list in reverse order")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the header information")
    (options, args) = parser.parse_args(args)
    if len(args) != 0:
        parser.error("This command takes no arguments")
    ensure_connection(session, goptions)
    opts = {}
    for key in ('type', 'prefix', 'pattern'):
        value = getattr(options, key)
        if value is not None:
            opts[key] = value
    if options.cg:
        opts['cgID'] = options.cg
    if options.package:
        try:
            opts['packageID'] = int(options.package)
        except ValueError:
            package = session.getPackageID(options.package)
            if package is None:
                parser.error("No such package: %s" % options.package)
            opts['packageID'] = package
    if options.owner:
        try:
            opts['userID'] = int(options.owner)
        except ValueError:
            user = session.getUser(options.owner)
            if user is None:
                parser.error("No such user: %s" % options.owner)
            opts['userID'] = user['id']
    if options.volume:
        try:
            opts['volumeID'] = int(options.volume)
        except ValueError:
            volumes = session.listVolumes()
            volumeID = None
            for volume in volumes:
                if options.volume == volume['name']:
                    volumeID = volume['id']
            if volumeID is None:
                parser.error("No such volume: %s" % options.volume)
            opts['volumeID'] = volumeID
    if options.state:
        try:
            state = int(options.state)
            if state > 4 or state < 0:
                parser.error("Invalid state: %s" % options.state)
            opts['state'] = state
        except ValueError:
            try:
                opts['state'] = koji.BUILD_STATES[options.state]
            except KeyError:
                parser.error("Invalid state: %s" % options.state)
    if options.before:
        opts['completeBefore'] = options.before
    if options.after:
        opts['completeAfter'] = options.after
    if options.task:
        try:
            opts['taskID'] = int(options.task)
        except ValueError:
            parser.error("Task id must be an integer")
    if options.source:
        opts['source'] = options.source
    if options.buildid:
        try:
            buildid = int(options.buildid)
        except ValueError:
            buildid = options.buildid
        data = [session.getBuild(buildid)]
        if data[0] is None:
            parser.error("No such build: '%s'" % buildid)
    else:
        # Check filter exists
        if any(opts):
            try:
                data = session.listBuilds(**opts)
            except koji.ParameterError as e:
                if e.args[0].endswith("'pattern'"):
                    parser.error("The hub doesn't support the 'pattern' argument, please try"
                                 " filtering the result on your local instead.")
                if e.args[0].endswith("'cgID'"):
                    parser.error("The hub doesn't support the 'cg' argument, please try"
                                 " filtering the result on your local instead.")
        else:
            parser.error("Filter must be provided for list")
    if not options.sort_key:
        options.sort_key = ['nvr']
    else:
        for s_key in options.sort_key:
            if s_key not in ['nvr', 'owner_name', 'state']:
                warn("Invalid sort_key: %s." % s_key)

    data = sorted(data, key=lambda b: [b.get(k) for k in options.sort_key],
                  reverse=options.reverse)
    for build in data:
        build['state'] = koji.BUILD_STATES[build['state']]

    fmt = "%(nvr)-55s  %(owner_name)-16s  %(state)s"
    if not options.quiet:
        print("%-55s  %-16s  %s" % ("Build", "Built by", "State"))
        print("%s  %s  %s" % ("-" * 55, "-" * 16, "-" * 16))

    for build in data:
        print(fmt % build)
