from __future__ import absolute_import, division

import sys
from optparse import OptionParser



from koji_cli.lib import (
    ensure_connection,
    format_inheritance_flags,
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


def _printInheritance(tags, sibdepths=None, reverse=False):
    if len(tags) == 0:
        return
    if sibdepths is None:
        sibdepths = []
    currtag = tags[0]
    tags = tags[1:]
    if reverse:
        siblings = len([tag for tag in tags if tag['parent_id'] == currtag['parent_id']])
    else:
        siblings = len([tag for tag in tags if tag['child_id'] == currtag['child_id']])

    sys.stdout.write(format_inheritance_flags(currtag))
    outdepth = 0
    for depth in sibdepths:
        if depth < currtag['currdepth']:
            outspacing = depth - outdepth
            sys.stdout.write(' ' * (outspacing * 3 - 1))
            sys.stdout.write(_printable_unicode(u'\u2502'))
            outdepth = depth

    sys.stdout.write(' ' * ((currtag['currdepth'] - outdepth) * 3 - 1))
    if siblings:
        sys.stdout.write(_printable_unicode(u'\u251c'))
    else:
        sys.stdout.write(_printable_unicode(u'\u2514'))
    sys.stdout.write(_printable_unicode(u'\u2500'))
    if reverse:
        sys.stdout.write('%(name)s (%(tag_id)i)\n' % currtag)
    else:
        sys.stdout.write('%(name)s (%(parent_id)i)\n' % currtag)

    if siblings:
        if len(sibdepths) == 0 or sibdepths[-1] != currtag['currdepth']:
            sibdepths.append(currtag['currdepth'])
    else:
        if len(sibdepths) > 0 and sibdepths[-1] == currtag['currdepth']:
            sibdepths.pop()

    _printInheritance(tags, sibdepths, reverse)


