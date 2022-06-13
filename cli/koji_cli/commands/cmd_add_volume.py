from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_volume(goptions, session, args):
    "[admin] Add a new storage volume"
    usage = "usage: %prog add-volume <volume-name>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Command requires exactly one volume-name.")
    name = args[0]
    volinfo = session.getVolume(name)
    if volinfo:
        error("Volume %s already exists" % name)
    activate_session(session, goptions)
    volinfo = session.addVolume(name)
    print("Added volume %(name)s with id %(id)i" % volinfo)


