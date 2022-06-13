from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_group(goptions, session, args):
    "[admin] Add a group to a tag"
    usage = "usage: %prog add-group <tag> <group>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) != 2:
        parser.error("Please specify a tag name and a group name")
    tag = args[0]
    group = args[1]

    activate_session(session, goptions)
    if not (session.hasPerm('admin') or session.hasPerm('tag')):
        parser.error("This action requires tag or admin privileges")

    dsttag = session.getTag(tag)
    if not dsttag:
        error("No such tag: %s" % tag)

    groups = dict([(p['name'], p['group_id']) for p in session.getTagGroups(tag, inherit=False)])
    group_id = groups.get(group, None)
    if group_id is not None:
        error("Group %s already exists for tag %s" % (group, tag))

    session.groupListAdd(tag, group)


