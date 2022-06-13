from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_targets(goptions, session, args):
    "[info] List the build targets"
    usage = "usage: %prog list-targets [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--name", help="Specify the build target name")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the header information")
    (options, args) = parser.parse_args(args)

    if len(args) != 0:
        parser.error("This command takes no arguments")
    ensure_connection(session, goptions)

    targets = session.getBuildTargets(options.name)
    if len(targets) == 0:
        if options.name:
            parser.error('No such build target: %s' % options.name)
        else:
            parser.error('No targets were found')

    fmt = "%(name)-30s %(build_tag_name)-30s %(dest_tag_name)-30s"
    if not options.quiet:
        print("%-30s %-30s %-30s" % ('Name', 'Buildroot', 'Destination'))
        print("-" * 93)
    tmp_list = sorted([(x['name'], x) for x in targets])
    targets = [x[1] for x in tmp_list]
    for target in targets:
        print(fmt % target)
    # pprint.pprint(session.getBuildTargets())
