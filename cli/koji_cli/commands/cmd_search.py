from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_search(goptions, session, args):
    "[search] Search the system"
    usage = "usage: %prog search [options] <search_type> <pattern>"
    usage += '\nAvailable search types: %s' % ', '.join(_search_types)
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-r", "--regex", action="store_true", help="treat pattern as regex")
    parser.add_option("--exact", action="store_true", help="exact matches only")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify search type")
    if len(args) < 2:
        parser.error("Please specify search pattern")
    type = args[0]
    if type not in _search_types:
        parser.error("No such search type: %s" % type)
    pattern = args[1]
    matchType = 'glob'
    if options.regex:
        matchType = 'regexp'
    elif options.exact:
        matchType = 'exact'
    ensure_connection(session, goptions)
    data = session.search(pattern, type, matchType)
    for row in data:
        print(row['name'])


