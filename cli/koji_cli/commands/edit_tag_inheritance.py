from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_edit_tag_inheritance(goptions, session, args):
    """[admin] Edit tag inheritance"""
    usage = "usage: %prog edit-tag-inheritance [options] <tag> <parent> <priority>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--priority", help="Specify a new priority")
    parser.add_option("--maxdepth", help="Specify max depth")
    parser.add_option("--intransitive", action="store_true", help="Set intransitive")
    parser.add_option("--noconfig", action="store_true", help="Set to packages only")
    parser.add_option("--pkg-filter", help="Specify the package filter")
    (options, args) = parser.parse_args(args)

    if len(args) < 1:
        parser.error("This command takes at least one argument: a tag name or ID")

    if len(args) > 3:
        parser.error("This command takes at most three argument: a tag name or ID, "
                     "a parent tag name or ID, and a priority")

    activate_session(session, goptions)

    tag = session.getTag(args[0])
    if not tag:
        parser.error("No such tag: %s" % args[0])

    parent = None
    priority = None
    if len(args) > 1:
        parent = session.getTag(args[1])
        if not parent:
            parser.error("No such tag: %s" % args[1])
        if len(args) > 2:
            priority = args[2]

    data = session.getInheritanceData(tag['id'])
    if parent and data:
        data = [datum for datum in data if datum['parent_id'] == parent['id']]
    if priority and data:
        data = [datum for datum in data if datum['priority'] == priority]

    if len(data) == 0:
        error("No inheritance link found to remove.  Please check your arguments")
    elif len(data) > 1:
        print("Multiple matches for tag.")
        if not parent:
            error("Please specify a parent on the command line.")
        if not priority:
            error("Please specify a priority on the command line.")
        error("Error: Key constraints may be broken.  Exiting.")

    # len(data) == 1
    data = data[0]

    inheritanceData = session.getInheritanceData(tag['id'])
    samePriority = [datum for datum in inheritanceData if datum['priority'] == options.priority]
    if samePriority:
        error("Error: There is already an active inheritance with that priority on %s, "
              "please specify a different priority with --priority." % tag['name'])

    new_data = data.copy()
    if options.priority is not None and options.priority.isdigit():
        new_data['priority'] = int(options.priority)
    if options.maxdepth is not None:
        if options.maxdepth.isdigit():
            new_data['maxdepth'] = int(options.maxdepth)
        elif options.maxdepth.lower() == "none":
            new_data['maxdepth'] = None
        else:
            error("Invalid maxdepth: %s" % options.maxdepth)
    if options.intransitive:
        new_data['intransitive'] = options.intransitive
    if options.noconfig:
        new_data['noconfig'] = options.noconfig
    if options.pkg_filter:
        new_data['pkg_filter'] = options.pkg_filter

    # find the data we want to edit and replace it
    index = inheritanceData.index(data)
    inheritanceData[index] = new_data
    session.setInheritanceData(tag['id'], inheritanceData)
