from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_remove_host_from_channel(goptions, session, args):
    "[admin] Remove a host from a channel"
    usage = "usage: %prog remove-host-from-channel [options] <hostname> <channel>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) != 2:
        parser.error("Please specify a hostname and a channel")
    host = args[0]
    activate_session(session, goptions)
    hostinfo = session.getHost(host)
    if not hostinfo:
        error("No such host: %s" % host)
    hostchannels = [c['name'] for c in session.listChannels(hostinfo['id'])]

    channel = args[1]
    if channel not in hostchannels:
        error("Host %s is not a member of channel %s" % (host, channel))

    session.removeHostFromChannel(host, channel)


