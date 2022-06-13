from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip

import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str,
    truncate_string
)


def anon_handle_list_channels(goptions, session, args):
    "[info] Print channels listing"
    usage = "usage: %prog list-channels [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--simple", action="store_true", default=False,
                      help="Print just list of channels without additional info")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print header information")
    parser.add_option("--comment", action="store_true", help="Show comments")
    parser.add_option("--description", action="store_true", help="Show descriptions")
    parser.add_option("--enabled", action="store_true", help="Limit to enabled channels")
    parser.add_option("--not-enabled", action="store_false", dest="enabled",
                      help="Limit to not enabled channels")
    parser.add_option("--disabled", action="store_false", dest="enabled",
                      help="Alias for --not-enabled")
    (options, args) = parser.parse_args(args)
    ensure_connection(session, goptions)
    opts = {}
    if options.enabled is not None:
        opts['enabled'] = options.enabled
    try:
        channels = sorted([x for x in session.listChannels(**opts)], key=lambda x: x['name'])
    except koji.ParameterError:
        channels = sorted([x for x in session.listChannels()], key=lambda x: x['name'])
    if len(channels) > 0:
        first_item = channels[0]
    else:
        first_item = {}
    session.multicall = True
    for channel in channels:
        session.listHosts(channelID=channel['id'])
    for channel, [hosts] in zip(channels, session.multiCall()):
        channel['enabled_host'] = len([x for x in hosts if x['enabled']])
        channel['disabled'] = len(hosts) - channel['enabled_host']
        channel['ready'] = len([x for x in hosts if x['ready']])
        channel['capacity'] = sum([x['capacity'] for x in hosts])
        channel['load'] = sum([x['task_load'] for x in hosts])
        if 'comment' in first_item:
            channel['comment'] = truncate_string(channel['comment'])
        if 'description' in first_item:
            channel['description'] = truncate_string(channel['description'])
        if channel['capacity']:
            channel['perc_load'] = channel['load'] / channel['capacity'] * 100.0
        else:
            channel['perc_load'] = 0.0
        if 'enabled' in first_item:
            if not channel['enabled']:
                channel['name'] = channel['name'] + ' [disabled]'
    if channels:
        longest_channel = max([len(ch['name']) for ch in channels])
    else:
        longest_channel = 8
    if options.simple:
        if not options.quiet:
            hdr = 'Channel'
            print(hdr)
            print(len(hdr) * '-')
        for channel in channels:
            print(channel['name'])
    else:
        if not options.quiet:
            hdr = '{channame:<{longest_channel}}Enabled  Ready Disbld   Load    Cap   ' \
                  'Perc    '
            hdr = hdr.format(longest_channel=longest_channel, channame='Channel')
            if options.description and 'description' in first_item:
                hdr += "Description".ljust(53)
            if options.comment and 'comment' in first_item:
                hdr += "Comment".ljust(53)
            print(hdr)
            print(len(hdr) * '-')
        mask = "%%(name)-%ss %%(enabled_host)6d %%(ready)6d %%(disabled)6d %%(load)6d %%(" \
               "capacity)6d %%(perc_load)6d%%%%" % longest_channel
        if options.description and 'description' in first_item:
            mask += "   %(description)-50s"
        if options.comment and 'comment' in first_item:
            mask += "   %(comment)-50s"
        for channel in channels:
            print(mask % channel)
