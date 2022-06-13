from __future__ import absolute_import, division

import random
from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str,
    greetings
)


def handle_moshimoshi(options, session, args):
    "[misc] Introduce yourself"
    usage = "usage: %prog moshimoshi [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    (opts, args) = parser.parse_args(args)
    if len(args) != 0:
        parser.error("This command takes no arguments")
    activate_session(session, options)
    u = session.getLoggedInUser()
    if not u:
        print("Not authenticated")
        u = {'name': 'anonymous user'}
    print("%s, %s!" % (_printable_unicode(random.choice(greetings)), u["name"]))
    print("")
    print("You are using the hub at %s" % session.baseurl)
    authtype = u.get('authtype', getattr(session, 'authtype', None))
    if authtype == koji.AUTHTYPE_NORMAL:
        print("Authenticated via password")
    elif authtype == koji.AUTHTYPE_GSSAPI:
        print("Authenticated via GSSAPI")
    elif authtype == koji.AUTHTYPE_KERB:
        print("Authenticated via Kerberos principal %s" % session.krb_principal)
    elif authtype == koji.AUTHTYPE_SSL:
        print("Authenticated via client certificate %s" % options.cert)


