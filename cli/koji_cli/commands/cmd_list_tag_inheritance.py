from __future__ import absolute_import, division

import sys
import textwrap
import time
from optparse import SUPPRESS_HELP, OptionParser


import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


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


