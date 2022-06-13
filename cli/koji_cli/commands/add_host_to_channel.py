from __future__ import absolute_import, division

from optparse import SUPPRESS_HELP, OptionParser

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_host_to_channel(goptions, session, args):
    "[admin] Add a host to a channel"
    usage = "usage: %prog add-host-to-channel [options] <hostname> <channel>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--list", action="store_true", help=SUPPRESS_HELP)
    parser.add_option("--new", action="store_true", help="Create channel if needed")
    parser.add_option("--force", action="store_true", help="force added, if possible")
    (options, args) = parser.parse_args(args)
    if not options.list and len(args) != 2:
        parser.error("Please specify a hostname and a channel")
    activate_session(session, goptions)
    if options.list:
        for channel in session.listChannels():
            print(channel['name'])
        return
    channel = args[1]
    if not options.new:
        channelinfo = session.getChannel(channel)
        if not channelinfo:
            error("No such channel: %s" % channel)

    host = args[0]
    hostinfo = session.getHost(host)
    if not hostinfo:
        error("No such host: %s" % host)
    kwargs = {}
    if options.new:
        kwargs['create'] = True
    if options.force:
        kwargs['force'] = True
    session.addHostToChannel(host, channel, **kwargs)
