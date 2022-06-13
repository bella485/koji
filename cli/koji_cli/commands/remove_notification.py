from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_remove_notification(goptions, session, args):
    "[monitor] Remove user's notifications"
    usage = "usage: %prog remove-notification [options] <notification_id> [<notification_id> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)

    activate_session(session, goptions)

    if len(args) < 1:
        parser.error("At least one notification id has to be specified")

    try:
        n_ids = [int(x) for x in args]
    except ValueError:
        parser.error("All notification ids has to be integers")

    for n_id in n_ids:
        session.deleteNotification(n_id)
        if not goptions.quiet:
            print("Notification %d successfully removed." % n_id)
