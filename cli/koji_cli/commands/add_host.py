from __future__ import absolute_import, division

from optparse import OptionParser

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_host(goptions, session, args):
    "[admin] Add a host"
    usage = "usage: %prog add-host [options] <hostname> <arch> [<arch> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--krb-principal",
                      help="set a non-default kerberos principal for the host")
    parser.add_option("--force", default=False, action="store_true",
                      help="if existing used is a regular user, convert it to a host")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a hostname and at least one arch")
    host = args[0]
    activate_session(session, goptions)
    id = session.getHost(host)
    if id:
        error("%s is already in the database" % host)
    else:
        kwargs = {'force': options.force}
        if options.krb_principal is not None:
            kwargs['krb_principal'] = options.krb_principal
        id = session.addHost(host, args[1:], **kwargs)
        print("%s added: id %d" % (host, id))
