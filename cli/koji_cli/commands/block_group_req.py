from __future__ import absolute_import, division

from optparse import OptionParser

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_block_group_req(goptions, session, args):
    "[admin] Block a group's requirement listing"
    usage = "usage: %prog block-group-req [options] <tag> <group> <blocked req>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) != 3:
        parser.error("You must specify a tag name and two group names")
    tag = args[0]
    group = args[1]
    req = args[2]
    activate_session(session, goptions)
    session.groupReqListBlock(tag, group, req)
