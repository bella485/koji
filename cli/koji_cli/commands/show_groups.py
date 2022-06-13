from __future__ import absolute_import, division

import pprint
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_show_groups(goptions, session, args):
    "[info] Show groups data for a tag"
    usage = "usage: %prog show-groups [options] <tag>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--comps", action="store_true", help="Print in comps format")
    parser.add_option("-x", "--expand", action="store_true", default=False,
                      help="Expand groups in comps format")
    parser.add_option("--spec", action="store_true", help="Print build spec")
    parser.add_option("--show-blocked", action="store_true", dest="incl_blocked",
                      help="Show blocked packages")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Incorrect number of arguments")
    if options.incl_blocked and (options.comps or options.spec):
        parser.error("--show-blocked doesn't make sense for comps/spec output")
    ensure_connection(session, goptions)
    tag = args[0]
    callopts = {}
    if options.incl_blocked:
        callopts['incl_blocked'] = True
    groups = session.getTagGroups(tag, **callopts)
    if options.comps:
        print(koji.generate_comps(groups, expand_groups=options.expand))
    elif options.spec:
        print(koji.make_groups_spec(groups, name='buildgroups', buildgroup='build'))
    else:
        pprint.pprint(groups)
