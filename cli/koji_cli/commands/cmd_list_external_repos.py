from __future__ import absolute_import, division

import time
from optparse import OptionParser


import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_external_repos(goptions, session, args):
    "[info] List external repos"
    usage = "usage: %prog list-external-repos [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--url", help="Select by url")
    parser.add_option("--name", help="Select by name")
    parser.add_option("--id", type="int", help="Select by id")
    parser.add_option("--tag", help="Select by tag")
    parser.add_option("--used", action='store_true', help="List which tags use the repo(s)")
    parser.add_option("--inherit", action='store_true',
                      help="Follow tag inheritance when selecting by tag")
    parser.add_option("--event", type='int', metavar="EVENT#", help="Query at event")
    parser.add_option("--ts", type='int', metavar="TIMESTAMP",
                      help="Query at last event before timestamp")
    parser.add_option("--repo", type='int', metavar="REPO#",
                      help="Query at event corresponding to (nonexternal) repo")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not display the column headers")
    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error("This command takes no arguments")
    ensure_connection(session, goptions)
    opts = {}
    event = koji.util.eventFromOpts(session, options)
    if event:
        opts['event'] = event['id']
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        print("Querying at event %(id)i (%(timestr)s)" % event)
    if options.tag:
        format = "tag"
        opts['tag_info'] = options.tag
        opts['repo_info'] = options.id or options.name or None
        if opts['repo_info']:
            if options.inherit:
                parser.error("Can't select by repo when using --inherit")
        if options.inherit:
            del opts['repo_info']
            data = session.getExternalRepoList(**opts)
            format = "multitag"
        else:
            data = session.getTagExternalRepos(**opts)
    elif options.used:
        format = "multitag"
        opts['repo_info'] = options.id or options.name or None
        data = session.getTagExternalRepos(**opts)
    else:
        format = "basic"
        opts['info'] = options.id or options.name or None
        opts['url'] = options.url or None
        data = session.listExternalRepos(**opts)

    # There are three different output formats
    #  1) Listing just repo data (name, url)
    #  2) Listing repo data for a tag (priority, name, url)
    #  3) Listing repo data for multiple tags (tag, priority, name, url)
    if format == "basic":
        format = "%(name)-25s %(url)s"
        header1 = "%-25s %s" % ("External repo name", "URL")
        header2 = "%s %s" % ("-" * 25, "-" * 40)
    elif format == "tag":
        format = "%(priority)-3i %(external_repo_name)-25s %(merge_mode)-10s %(url)s"
        header1 = "%-3s %-25s %-10s URL" % ("Pri", "External repo name", "Mode")
        header2 = "%s %s %s %s" % ("-" * 3, "-" * 25, "-" * 10, "-" * 40)
    elif format == "multitag":
        format = "%(tag_name)-20s %(priority)-3i %(merge_mode)-10s %(external_repo_name)s"
        header1 = "%-20s %-3s %-10s %s" % ("Tag", "Pri", "Mode", "External repo name")
        header2 = "%s %s %s %s" % ("-" * 20, "-" * 3, "-" * 10, "-" * 25)
    if not options.quiet:
        print(header1)
        print(header2)
    for rinfo in data:
        # older hubs do not support merge_mode
        rinfo.setdefault('merge_mode', None)
        print(format % rinfo)


def _pick_external_repo_priority(session, tag):
    """pick priority after current ones, leaving space for later insertions"""
    repolist = session.getTagExternalRepos(tag_info=tag)
    # ordered by priority
    if not repolist:
        priority = 5
    else:
        priority = (repolist[-1]['priority'] + 7) // 5 * 5
        # at least 3 higher than current max and a multiple of 5
    return priority


def _parse_tagpri(tagpri):
    parts = tagpri.rsplit('::', 1)
    tag = parts[0]
    if len(parts) == 1:
        return tag, None
    elif parts[1] in ('auto', '-1'):
        return tag, None
    else:
        try:
            pri = int(parts[1])
        except ValueError:
            raise koji.GenericError("Invalid priority: %s" % parts[1])
        return tag, pri


