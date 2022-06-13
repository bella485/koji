from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_remove_tag(goptions, session, args):
    "[admin] Remove a tag"
    usage = "usage: %prog remove-tag [options] <name>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)

    if len(args) != 1:
        parser.error("Please specify a tag to remove")
    activate_session(session, goptions)

    if not (session.hasPerm('admin') or session.hasPerm('tag')):
        parser.error("This action requires tag or admin privileges")

    tag = args[0]
    tag_info = session.getTag(tag)
    if not tag_info:
        error("No such tag: %s" % tag)

    session.deleteTag(tag_info['id'])


