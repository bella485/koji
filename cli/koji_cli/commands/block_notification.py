from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_block_notification(goptions, session, args):
    "[monitor] Block user's notifications"
    usage = "usage: %prog block-notification [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--user", help="Block notifications for this user (admin-only)")
    parser.add_option("--package", help="Block notifications for this package")
    parser.add_option("--tag", help="Block notifications for this tag")
    parser.add_option("--all", action="store_true", help="Block all notification for this user")
    (options, args) = parser.parse_args(args)

    if len(args) != 0:
        parser.error("This command takes no arguments")

    if not options.package and not options.tag and not options.all:
        parser.error("One of --tag, --package or --all must be specified.")

    activate_session(session, goptions)

    if options.user and not session.hasPerm('admin'):
        parser.error("--user requires admin permission")

    if options.user:
        user_id = session.getUser(options.user, strict=True)['id']
    else:
        logged_in_user = session.getLoggedInUser()
        if logged_in_user:
            user_id = logged_in_user['id']
        else:
            parser.error("Please login with authentication or specify --user")

    if options.package:
        package_id = session.getPackageID(options.package)
        if package_id is None:
            parser.error("No such package: %s" % options.package)
    else:
        package_id = None

    if options.tag:
        try:
            tag_id = session.getTagID(options.tag, strict=True)
        except koji.GenericError:
            parser.error("No such tag: %s" % options.tag)
    else:
        tag_id = None

    for block in session.getBuildNotificationBlocks(user_id):
        if block['package_id'] == package_id and block['tag_id'] == tag_id:
            parser.error('Notification already exists.')

    session.createNotificationBlock(user_id, package_id, tag_id)
