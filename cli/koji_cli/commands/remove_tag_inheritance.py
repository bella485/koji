from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_remove_tag_inheritance(goptions, session, args):
    """[admin] Remove a tag inheritance link"""
    usage = "usage: %prog remove-tag-inheritance <tag> <parent> <priority>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)

    if len(args) < 1:
        parser.error("This command takes at least one argument: a tag name or ID")

    if len(args) > 3:
        parser.error("This command takes at most three argument: a tag name or ID, a parent tag "
                     "name or ID, and a priority")

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

    new_data = data.copy()
    new_data['delete link'] = True

    # find the data we want to edit and replace it
    index = inheritanceData.index(data)
    inheritanceData[index] = new_data
    session.setInheritanceData(tag['id'], inheritanceData)
