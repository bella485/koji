from __future__ import absolute_import, division

import koji

from koji_cli.lib import (
    ensure_connection
)


def handle_version(goptions, session, args):
    """Report client and hub versions"""
    ensure_connection(session, goptions)
    print('Client: %s' % koji.__version__)
    try:
        version = session.getKojiVersion()
        print("Hub:    %s" % version)
    except koji.GenericError:
        print("Hub:    Can't determine (older than 1.23)")
