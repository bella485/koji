from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_edit_channel(goptions, session, args):
    "[admin] Edit a channel"
    usage = "usage: %prog edit-channel [options] <old-name>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--name", help="New channel name")
    parser.add_option("--description", help="Description of channel")
    parser.add_option("--comment", help="Comment of channel")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Incorrect number of arguments")
    activate_session(session, goptions)
    vals = {}
    for key, val in options.__dict__.items():
        if val is not None:
            vals[key] = val
    cinfo = session.getChannel(args[0])
    if not cinfo:
        error("No such channel: %s" % args[0])
    try:
        result = session.editChannel(args[0], **vals)
    except koji.GenericError as ex:
        msg = str(ex)
        if 'Invalid method:' in msg:
            version = session.getKojiVersion()
            error("editChannel is available on hub from Koji 1.26 version, your version is %s" %
                  version)
        else:
            print(msg)
    if not result:
        error("No changes made, please correct the command line")
