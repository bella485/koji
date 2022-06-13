from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_volumes(goptions, session, args):
    "[info] List storage volumes"
    usage = "usage: %prog list-volumes"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    ensure_connection(session, goptions)
    for volinfo in session.listVolumes():
        print(volinfo['name'])


