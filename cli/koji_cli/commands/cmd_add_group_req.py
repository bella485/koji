from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_add_group_req(goptions, session, args):
    "[admin] Add a group to a group's required list"
    usage = "usage: %prog add-group-req [options] <tag> <target group> <required group>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) != 3:
        parser.error("You must specify a tag name and two group names")
    tag = args[0]
    group = args[1]
    req = args[2]
    activate_session(session, goptions)
    session.groupReqListAdd(tag, group, req)


