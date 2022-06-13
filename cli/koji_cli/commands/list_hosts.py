from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip

import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str,
    warn,
    truncate_string
)


def anon_handle_list_hosts(goptions, session, args):
    "[info] Print the host listing"
    usage = "usage: %prog list-hosts [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arch", action="append", default=[], help="Specify an architecture")
    parser.add_option("--channel", help="Specify a channel")
    parser.add_option("--ready", action="store_true", help="Limit to ready hosts")
    parser.add_option("--not-ready", action="store_false", dest="ready",
                      help="Limit to not ready hosts")
    parser.add_option("--enabled", action="store_true", help="Limit to enabled hosts")
    parser.add_option("--not-enabled", action="store_false", dest="enabled",
                      help="Limit to not enabled hosts")
    parser.add_option("--disabled", action="store_false", dest="enabled",
                      help="Alias for --not-enabled")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print header information")
    parser.add_option("--show-channels", action="store_true", help="Show host's channels")
    parser.add_option("--comment", action="store_true", help="Show comments")
    parser.add_option("--description", action="store_true", help="Show descriptions")
    (options, args) = parser.parse_args(args)
    opts = {}
    ensure_connection(session, goptions)
    if options.arch:
        opts['arches'] = options.arch
    if options.channel:
        channel = session.getChannel(options.channel)
        if not channel:
            parser.error('No such channel: %s' % options.channel)
        opts['channelID'] = channel['id']
    if options.ready is not None:
        opts['ready'] = options.ready
    if options.enabled is not None:
        opts['enabled'] = options.enabled
    tmp_list = sorted([(x['name'], x) for x in session.listHosts(**opts)])
    hosts = [x[1] for x in tmp_list]

    if not hosts:
        warn("No hosts found.")
        return

    def yesno(x):
        if x:
            return 'Y'
        else:
            return 'N'

    try:
        first = session.getLastHostUpdate(hosts[0]['id'], ts=True)
        opts = {'ts': True}
    except koji.ParameterError:
        # Hubs prior to v1.25.0 do not have a "ts" parameter for getLastHostUpdate
        first = session.getLastHostUpdate(hosts[0]['id'])
        opts = {}

    # pull in the last update using multicall to speed it up a bit
    with session.multicall() as m:
        result = [m.getLastHostUpdate(host['id'], **opts) for host in hosts[1:]]
    updateList = [first] + [x.result for x in result]

    for host, update in zip(hosts, updateList):
        if update is None:
            host['update'] = '-'
        else:
            host['update'] = koji.formatTimeLong(update)
        host['enabled'] = yesno(host['enabled'])
        host['ready'] = yesno(host['ready'])
        host['arches'] = ','.join(host['arches'].split())
        host['description'] = truncate_string(host['description'])
        host['comment'] = truncate_string(host['comment'])

    # pull hosts' channels
    if options.show_channels:
        with session.multicall() as m:
            result = [m.listChannels(host['id']) for host in hosts]
        first_line_channel = result[0].result[0]
        for host, channels in zip(hosts, result):
            list_channels = []
            for c in channels.result:
                if 'enabled' in first_line_channel:
                    if c['enabled']:
                        list_channels.append(c['name'])
                    else:
                        list_channels.append('*' + c['name'])
                else:
                    list_channels.append(c['name'])
            host['channels'] = ','.join(sorted(list_channels))

    if hosts:
        longest_host = max([len(h['name']) for h in hosts])
    else:
        longest_host = 8

    if not options.quiet:
        hdr = "{hostname:<{longest_host}} Enb Rdy Load/Cap  Arches           " \
              "Last Update                         "
        hdr = hdr.format(longest_host=longest_host, hostname='Hostname')
        if options.description:
            hdr += "Description".ljust(51)
        if options.comment:
            hdr += "Comment".ljust(51)
        if options.show_channels:
            hdr += "Channels"
        print(hdr)
        print(len(hdr) * '-')
    mask = "%%(name)-%ss %%(enabled)-3s %%(ready)-3s %%(task_load)4.1f/%%(capacity)-4.1f " \
           "%%(arches)-16s %%(update)-35s" % longest_host
    if options.description:
        mask += " %(description)-50s"
    if options.comment:
        mask += " %(comment)-50s"
    if options.show_channels:
        mask += " %(channels)s"
    for host in hosts:
        print(mask % host)
