from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_edit_notification(goptions, session, args):
    "[monitor] Edit user's notification"
    usage = "usage: %prog edit-notification [options] <notification_id>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--package", help="Notifications for this package, '*' for all")
    parser.add_option("--tag", help="Notifications for this tag, '*' for all")
    parser.add_option("--success-only", action="store_true", default=None,
                      dest='success_only', help="Notify only on successful events")
    parser.add_option("--no-success-only", action="store_false",
                      default=None, dest='success_only', help="Notify on all events")
    (options, args) = parser.parse_args(args)

    if len(args) != 1:
        parser.error("Only argument is notification ID")

    try:
        n_id = int(args[0])
    except ValueError:
        parser.error("Notification ID has to be numeric")

    if not options.package and not options.tag and options.success_only is None:
        parser.error("Command need at least one option")

    activate_session(session, goptions)

    old = session.getBuildNotification(n_id)

    if options.package == '*':
        package_id = None
    elif options.package:
        package_id = session.getPackageID(options.package)
        if package_id is None:
            parser.error("No such package: %s" % options.package)
    else:
        package_id = old['package_id']

    if options.tag == '*':
        tag_id = None
    elif options.tag:
        try:
            tag_id = session.getTagID(options.tag, strict=True)
        except koji.GenericError:
            parser.error("No such tag: %s" % options.tag)
    else:
        tag_id = old['tag_id']

    if options.success_only is not None:
        success_only = options.success_only
    else:
        success_only = old['success_only']

    session.updateNotification(n_id, package_id, tag_id, success_only)
