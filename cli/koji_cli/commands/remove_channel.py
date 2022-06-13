from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_remove_channel(goptions, session, args):
    "[admin] Remove a channel entirely"
    usage = "usage: %prog remove-channel [options] <channel>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action="store_true", help="force removal, if possible")
    (options, args) = parser.parse_args(args)
    print("remove-channel is deprecated and will be removed in 1.30")
    if len(args) != 1:
        parser.error("Incorrect number of arguments")
    activate_session(session, goptions)
    cinfo = session.getChannel(args[0])
    if not cinfo:
        error("No such channel: %s" % args[0])
    session.removeChannel(args[0], force=options.force)
