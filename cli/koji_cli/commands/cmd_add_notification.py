from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_add_notification(goptions, session, args):
    "[monitor] Add user's notification"
    usage = "usage: %prog add-notification [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--user", help="Add notifications for this user (admin-only)")
    parser.add_option("--package", help="Add notifications for this package")
    parser.add_option("--tag", help="Add notifications for this tag")
    parser.add_option("--success-only", action="store_true", default=False, help="")
    (options, args) = parser.parse_args(args)

    if len(args) != 0:
        parser.error("This command takes no arguments")

    if not options.package and not options.tag:
        parser.error("Command need at least one from --tag or --package options.")

    activate_session(session, goptions)

    if options.user and not session.hasPerm('admin'):
        parser.error("--user requires admin permission")

    if options.user:
        user_id = session.getUser(options.user)['id']
    else:
        user_id = session.getLoggedInUser()['id']

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

    session.createNotification(user_id, package_id, tag_id, options.success_only)


