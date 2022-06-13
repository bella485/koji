from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip

import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_edit_host(options, session, args):
    "[admin] Edit a host"
    usage = "usage: %prog edit-host <hostname> [<hostname> ...] [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arches",
                      help="Space or comma-separated list of supported architectures")
    parser.add_option("--capacity", type="float", help="Capacity of this host")
    parser.add_option("--description", metavar="DESC", help="Description of this host")
    parser.add_option("--comment", help="A brief comment about this host")
    (subopts, args) = parser.parse_args(args)
    if not args:
        parser.error("Please specify a hostname")

    activate_session(session, options)

    vals = {}
    for key, val in subopts.__dict__.items():
        if val is not None:
            vals[key] = val
    if 'arches' in vals:
        vals['arches'] = koji.parse_arches(vals['arches'])

    session.multicall = True
    for host in args:
        session.getHost(host)
    error_hit = False
    for host, [info] in zip(args, session.multiCall(strict=True)):
        if not info:
            warn("No such host: %s" % host)
            error_hit = True

    if error_hit:
        error("No changes made, please correct the command line")

    session.multicall = True
    for host in args:
        session.editHost(host, **vals)
    for host, [result] in zip(args, session.multiCall(strict=True)):
        if result:
            print("Edited %s" % host)
        else:
            print("No changes made to %s" % host)
