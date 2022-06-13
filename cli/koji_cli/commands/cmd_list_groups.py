from __future__ import absolute_import, division

import time
from optparse import OptionParser


import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_groups(goptions, session, args):
    "[info] Print the group listings"
    usage = "usage: %prog list-groups [options] <tag> [<group>]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--event", type='int', metavar="EVENT#", help="query at event")
    parser.add_option("--ts", type='int', metavar="TIMESTAMP",
                      help="query at last event before timestamp")
    parser.add_option("--repo", type='int', metavar="REPO#", help="query at event for a repo")
    parser.add_option("--show-blocked", action="store_true", dest="incl_blocked",
                      help="Show blocked packages and groups")
    (options, args) = parser.parse_args(args)
    if len(args) < 1 or len(args) > 2:
        parser.error("Incorrect number of arguments")
    opts = {}
    if options.incl_blocked:
        opts['incl_blocked'] = True
    ensure_connection(session, goptions)
    event = koji.util.eventFromOpts(session, options)
    if event:
        opts['event'] = event['id']
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        print("Querying at event %(id)i (%(timestr)s)" % event)
    tmp_list = sorted([(x['name'], x) for x in session.getTagGroups(args[0], **opts)])
    groups = [x[1] for x in tmp_list]

    tags_cache = {}

    def get_cached_tag(tag_id):
        if tag_id not in tags_cache:
            tag = session.getTag(tag_id, strict=False)
            if tag is None:
                tags_cache[tag_id] = tag_id
            else:
                tags_cache[tag_id] = tag['name']
        return tags_cache[tag_id]

    for group in groups:
        if len(args) > 1 and group['name'] != args[1]:
            continue
        print("%s  [%s]" % (group['name'], get_cached_tag(group['tag_id'])))
        groups = sorted([(x['name'], x) for x in group['grouplist']])
        for x in [x[1] for x in groups]:
            x['tag_name'] = get_cached_tag(x['tag_id'])
            print_group_list_req_group(x)
        pkgs = sorted([(x['package'], x) for x in group['packagelist']])
        for x in [x[1] for x in pkgs]:
            x['tag_name'] = get_cached_tag(x['tag_id'])
            print_group_list_req_package(x)


