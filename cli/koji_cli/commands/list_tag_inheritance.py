from __future__ import absolute_import, division

import sys
import textwrap
import time
from optparse import SUPPRESS_HELP, OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    format_inheritance_flags,
    get_usage_str,
    printable_unicode,
)


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
            sys.stdout.write(printable_unicode(u'\u2502'))
            outdepth = depth

    sys.stdout.write(' ' * ((currtag['currdepth'] - outdepth) * 3 - 1))
    if siblings:
        sys.stdout.write(printable_unicode(u'\u251c'))
    else:
        sys.stdout.write(printable_unicode(u'\u2514'))
    sys.stdout.write(printable_unicode(u'\u2500'))
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


def anon_handle_list_tag_inheritance(goptions, session, args):
    "[info] Print the inheritance information for a tag"
    usage = """\
        usage: %prog list-tag-inheritance [options] <tag>

        Prints tag inheritance with basic information about links.
        Four flags could be seen in the output:
         M - maxdepth - limits inheritance to n-levels
         F - package filter (packages ignored for inheritance)
         I - intransitive link - inheritance immediately stops here
         N - noconfig - if tag is used in buildroot, its configuration values will not be used

        Exact values for maxdepth and package filter can be inquired by taginfo command.
    """
    usage = textwrap.dedent(usage)
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--reverse", action="store_true",
                      help="Process tag's children instead of its parents")
    parser.add_option("--stop", help=SUPPRESS_HELP)
    parser.add_option("--jump", help=SUPPRESS_HELP)
    parser.add_option("--event", type='int', metavar="EVENT#", help="query at event")
    parser.add_option("--ts", type='int', metavar="TIMESTAMP",
                      help="query at last event before timestamp")
    parser.add_option("--repo", type='int', metavar="REPO#", help="query at event for a repo")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("This command takes exactly one argument: a tag name or ID")
    for deprecated in ('stop', 'jump'):
        if getattr(options, deprecated):
            parser.error("--%s option has been removed in 1.26" % deprecated)
    ensure_connection(session, goptions)
    event = koji.util.eventFromOpts(session, options)
    if event:
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        print("Querying at event %(id)i (%(timestr)s)" % event)
    if event:
        tag = session.getTag(args[0], event=event['id'])
    else:
        tag = session.getTag(args[0])
    if not tag:
        parser.error("No such tag: %s" % args[0])

    opts = {}
    opts['reverse'] = options.reverse or False
    if event:
        opts['event'] = event['id']

    sys.stdout.write('     %s (%i)\n' % (tag['name'], tag['id']))
    data = session.getFullInheritance(tag['id'], **opts)
    _printInheritance(data, None, opts['reverse'])
