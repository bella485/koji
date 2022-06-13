from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_disable_channel(goptions, session, args):
    "[admin] Mark one or more channels as disabled"
    usage = "usage: %prog disable-channel [options] <channelname> [<channelname> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--comment", help="Comment indicating why the channel(s) are being disabled")
    (options, args) = parser.parse_args(args)

    if not args:
        parser.error("At least one channel must be specified")

    activate_session(session, goptions)

    with session.multicall() as m:
        result = [m.getChannel(channel, strict=False) for channel in args]
    error_hit = False
    for channel, id in zip(args, result):
        if not id.result:
            print("No such channel: %s" % channel)
            error_hit = True
    if error_hit:
        error("No changes made. Please correct the command line.")
    with session.multicall() as m:
        [m.disableChannel(channel, comment=options.comment) for channel in args]


