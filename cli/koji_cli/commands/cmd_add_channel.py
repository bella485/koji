from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_channel(goptions, session, args):
    "[admin] Add a channel"
    usage = "usage: %prog add-channel [options] <channel_name>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--description", help="Description of channel")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Please specify one channel name")
    activate_session(session, goptions)
    channel_name = args[0]
    try:
        channel_id = session.addChannel(channel_name, description=options.description)
    except koji.GenericError as ex:
        msg = str(ex)
        if 'channel %s already exists' % channel_name in msg:
            error("channel %s already exists" % channel_name)
        elif 'Invalid method:' in msg:
            version = session.getKojiVersion()
            error("addChannel is available on hub from Koji 1.26 version, your version is %s" %
                  version)
        else:
            error(msg)
    print("%s added: id %d" % (args[0], channel_id))


