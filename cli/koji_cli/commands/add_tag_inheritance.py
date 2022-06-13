from __future__ import absolute_import, division

from optparse import OptionParser

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_add_tag_inheritance(goptions, session, args):
    """[admin] Add to a tag's inheritance"""
    usage = "usage: %prog add-tag-inheritance [options] <tag> <parent-tag>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--priority", help="Specify priority")
    parser.add_option("--maxdepth", help="Specify max depth")
    parser.add_option("--intransitive", action="store_true", help="Set intransitive")
    parser.add_option("--noconfig", action="store_true", help="Set to packages only")
    parser.add_option("--pkg-filter", help="Specify the package filter")
    parser.add_option("--force", action="store_true",
                      help="Force adding a parent to a tag that already has that parent tag")
    (options, args) = parser.parse_args(args)

    if len(args) != 2:
        parser.error("This command takes exctly two argument: a tag name or ID and that tag's "
                     "new parent name or ID")

    activate_session(session, goptions)

    tag = session.getTag(args[0])
    if not tag:
        parser.error("No such tag: %s" % args[0])

    parent = session.getTag(args[1])
    if not parent:
        parser.error("No such tag: %s" % args[1])

    inheritanceData = session.getInheritanceData(tag['id'])
    priority = options.priority and int(options.priority) or 0
    sameParents = [datum for datum in inheritanceData if datum['parent_id'] == parent['id']]
    samePriority = [datum for datum in inheritanceData if datum['priority'] == priority]

    if sameParents and not options.force:
        warn("Error: You are attempting to add %s as %s's parent even though it already is "
             "%s's parent."
             % (parent['name'], tag['name'], tag['name']))
        error("Please use --force if this is what you really want to do.")
    if samePriority:
        error("Error: There is already an active inheritance with that priority on %s, "
              "please specify a different priority with --priority." % tag['name'])

    new_data = {}
    new_data['parent_id'] = parent['id']
    new_data['priority'] = options.priority or 0
    if options.maxdepth and options.maxdepth.isdigit():
        new_data['maxdepth'] = int(options.maxdepth)
    else:
        new_data['maxdepth'] = None
    new_data['intransitive'] = options.intransitive or False
    new_data['noconfig'] = options.noconfig or False
    new_data['pkg_filter'] = options.pkg_filter or ''

    inheritanceData.append(new_data)
    session.setInheritanceData(tag['id'], inheritanceData)
