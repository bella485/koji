from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import zip


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_disable_host(goptions, session, args):
    "[admin] Mark one or more hosts as disabled"
    usage = "usage: %prog disable-host [options] <hostname> [<hostname> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--comment", help="Comment indicating why the host(s) are being disabled")
    (options, args) = parser.parse_args(args)

    if not args:
        parser.error("At least one host must be specified")

    activate_session(session, goptions)
    session.multicall = True
    for host in args:
        session.getHost(host)
    error_hit = False
    for host, [id] in zip(args, session.multiCall(strict=True)):
        if not id:
            print("No such host: %s" % host)
            error_hit = True
    if error_hit:
        error("No changes made. Please correct the command line.")
    session.multicall = True
    for host in args:
        session.disableHost(host)
        if options.comment is not None:
            session.editHost(host, comment=options.comment)
    session.multiCall(strict=True)


