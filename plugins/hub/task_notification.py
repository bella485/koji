# koji hub plugin to trigger task notification.
# by default, only failed MavenTask can trigger a TaskNotification,
# which is exported as a task handler in a relative koji builder plugin.


import ConfigParser
import re
import sys

import six

from koji.context import context
from koji.plugin import callback, ignore_error
from koji.util import multi_fnmatch

# XXX - have to import kojihub for make_task
sys.path.insert(0, '/usr/share/koji-hub/')
import kojihub

__all__ = ('task_notification',)

CONFIG_FILE = '/etc/koji-hub/plugins/task_notification.conf'
FILTERS = {'methods': (['*'], None),
           'disallowed_methods': ([], ['*Notification']),
           'states': (['*'], None)}


def read_config():
    result = {}
    config = ConfigParser.SafeConfigParser()
    config.read(CONFIG_FILE)
    for k, (default, force) in six.iteritems(FILTERS):
        try:
            value = config.get('filters', k)
            value = re.split(r'[\s,]+', value)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            value = default
        if force is not None:
            value.extend(force)
        result[k] = value
    return result


def task_notification(task_id):
    """Trigger a notification of a task to the owner via email"""
    if context.opts.get('DisableNotifications'):
        return
    # sanity check for the existence of task
    taskinfo = kojihub.Task(task_id).getInfo(strict=True)
    # only send notification to the task's owner
    owner = kojihub.get_user(taskinfo['owner'], strict=True)['name']
    email_domain = context.opts['EmailDomain']
    recipient = '%s@%s' % (owner, email_domain)
    web_url = context.opts.get('KojiWebURL', 'http://localhost/koji')
    kojihub.make_task("taskNotification", [recipient, task_id, web_url])


@callback('postTaskStateChange')
@ignore_error
def task_notification_callback(cbtype, *args, **kws):
    if kws['attribute'] != 'state':
        return
    cfg = read_config()
    taskinfo = kws['info']
    new = kws['new']
    if multi_fnmatch(new, cfg['states']) \
            and multi_fnmatch(taskinfo['method'], cfg['methods']) \
            and not multi_fnmatch(taskinfo['method'], cfg['disallowed_methods']):
        task_notification(taskinfo['id'])
