from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import range

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str,
    warn
)


def anon_handle_hostinfo(goptions, session, args):
    "[info] Print basic information about a host"
    usage = "usage: %prog hostinfo [options] <hostname> [<hostname> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify a host")
    ensure_connection(session, goptions)
    error_hit = False
    for host in args:
        if host.isdigit():
            host = int(host)
        info = session.getHost(host)
        if info is None:
            warn("No such host: %s\n" % host)
            error_hit = True
            continue
        print("Name: %(name)s" % info)
        print("ID: %(id)d" % info)
        print("Arches: %(arches)s" % info)
        print("Capacity: %(capacity)s" % info)
        print("Task Load: %(task_load).2f" % info)
        if info['description']:
            description = info['description'].splitlines()
            print("Description: %s" % description[0])
            for line in description[1:]:
                print("%s%s" % (" " * 13, line))
        else:
            print("Description:")
        if info['comment']:
            comment = info['comment'].splitlines()
            print("Comment: %s" % comment[0])
            for line in comment[1:]:
                print("%s%s" % (" " * 9, line))
        else:
            print("Comment:")
        print("Enabled: %s" % (info['enabled'] and 'yes' or 'no'))
        print("Ready: %s" % (info['ready'] and 'yes' or 'no'))
        try:
            update = session.getLastHostUpdate(info['id'], ts=True)
        except koji.ParameterError:
            # Hubs prior to v1.25.0 do not have a "ts" parameter for getLastHostUpdate
            update = session.getLastHostUpdate(info['id'])
        if update is None:
            update = "never"
        else:
            update = koji.formatTimeLong(update)
        print("Last Update: %s" % update)
        print("Channels: %s" % ' '.join([c['name']
                                         for c in session.listChannels(hostID=info['id'])]))
        print("Active Buildroots:")
        states = {0: "INIT", 1: "WAITING", 2: "BUILDING"}
        rows = [('NAME', 'STATE', 'CREATION TIME')]
        for s in range(0, 3):
            for b in session.listBuildroots(hostID=info['id'], state=s):
                rows.append((("%s-%s-%s" % (b['tag_name'], b['id'], b['repo_id'])), states[s],
                             b['create_event_time'][:b['create_event_time'].find('.')]))
        if len(rows) > 1:
            for row in rows:
                print("%-50s %-10s %-20s" % row)
        else:
            print("None")
    if error_hit:
        error()
