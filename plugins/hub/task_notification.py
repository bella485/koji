# koji hub plugin to trigger task notification.
# by default, only failed MavenTask can trigger a TaskNotification,
# which is exported as a task handler in a relative koji builder plugin.

from koji.context import context
from koji.plugin import callback, ignore_error
import ConfigParser
import sys

# XXX - have to import kojihub for make_task
sys.path.insert(0, '/usr/share/koji-hub/')
import kojihub

__all__ = ('task_notification',)

CONFIG_FILE = '/etc/koji-hub/plugins/task_notification.conf'
config = None
allowed_methods = '*'
disallowed_methods = ['taskNotification']
allowed_states = '*'


def read_config():
    global config, allowed_methods, disallowed_methods, allowed_states
    # read configuration only once
    if config is None:
        config = ConfigParser.SafeConfigParser()
        config.read(CONFIG_FILE)
        allowed_methods = config.get('permissions', 'allowed_methods').split(',')
        if len(allowed_methods) == 1 and allowed_methods[0] == '*':
            allowed_methods = '*'
        allowed_states = config.get('permissions', 'allowed_states').split(',')
        if len(allowed_states) == 1 and allowed_states[0] == '*':
            allowed_states = '*'


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
    global allowed_methods, disallowed_methods, allowed_states
    if kws['attribute'] != 'state':
        return
    read_config()
    taskinfo = kws['info']
    new = kws['new']
    if (allowed_states == '*' or new in allowed_states) \
            and (allowed_methods == '*' or taskinfo['method'] in allowed_methods) \
            and taskinfo['method'] not in disallowed_methods:
        task_notification(taskinfo['id'])
