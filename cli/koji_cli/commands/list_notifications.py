from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    ensure_connection,
    error,
    get_usage_str
)


def anon_handle_list_notifications(goptions, session, args):
    "[monitor] List user's notifications and blocks"
    usage = "usage: %prog list-notifications [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--mine", action="store_true", help="Just print your notifications")
    parser.add_option("--user", help="Only notifications for this user")
    (options, args) = parser.parse_args(args)

    if len(args) != 0:
        parser.error("This command takes no arguments")
    if not options.mine and not options.user:
        parser.error("Use --user or --mine.")

    if options.user:
        ensure_connection(session, goptions)
        user = session.getUser(options.user)
        if not user:
            error("No such user: %s" % options.user)
        user_id = user['id']
    else:
        activate_session(session, goptions)
        user_id = None

    mask = "%(id)6s %(tag)-25s %(package)-25s %(email)-20s %(success)-12s"
    headers = {'id': 'ID',
               'tag': 'Tag',
               'package': 'Package',
               'email': 'E-mail',
               'success': 'Success-only'}
    head = mask % headers
    notifications = session.getBuildNotifications(user_id)
    if notifications:
        print('Notifications')
        print(head)
        print('-' * len(head))
        for notification in notifications:
            if notification['tag_id']:
                notification['tag'] = session.getTag(notification['tag_id'])['name']
            else:
                notification['tag'] = '*'
            if notification['package_id']:
                notification['package'] = session.getPackage(notification['package_id'])['name']
            else:
                notification['package'] = '*'
            notification['success'] = ['no', 'yes'][notification['success_only']]
            print(mask % notification)
    else:
        print('No notifications')

    print('')

    mask = "%(id)6s %(tag)-25s %(package)-25s"
    head = mask % headers
    blocks = session.getBuildNotificationBlocks(user_id)
    if blocks:
        print('Notification blocks')
        print(head)
        print('-' * len(head))
        for notification in blocks:
            if notification['tag_id']:
                notification['tag'] = session.getTag(notification['tag_id'])['name']
            else:
                notification['tag'] = '*'
            if notification['package_id']:
                notification['package'] = session.getPackage(notification['package_id'])['name']
            else:
                notification['package'] = '*'
            print(mask % notification)
    else:
        print('No notification blocks')
