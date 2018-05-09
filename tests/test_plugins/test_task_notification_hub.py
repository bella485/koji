import unittest

import mock

from koji import GenericError
from koji.context import context
from . import load_plugin
from .helper import FakeConfigParser

task_notification = load_plugin.load_plugin('hub', 'task_notification')

CONFIG1 = {'filters': {
    'methods': "someMethod,someotherMethod",
    'disallowed_methods': 'disallowedMethods,*Whatever',
    'states': "FAILED,NEW"
}}

CONFIG2 = {'filters': {
    'methods': "*",
    'states': "*"
}}


class TestTaskNotificationCallback(unittest.TestCase):
    def setUp(self):
        context.session = mock.MagicMock()
        context.opts = {'DisableNotifications': False,
                        'EmailDomain': 'example.com',
                        'KojiWebURL': 'https://koji.org'}
        self.parser = mock.patch('ConfigParser.SafeConfigParser',
                                 return_value=FakeConfigParser(
                                     CONFIG1)).start()
        self.getTaskInfo = mock.patch('kojihub.Task.getInfo').start()
        self.get_user = mock.patch('kojihub.get_user',
                                   return_value={'id': 999, 'name': 'someone',
                                                 'status': 0,
                                                 'usertype': 0}).start()
        self.make_task = mock.patch('kojihub.make_task').start()

    def tearDown(self):
        mock.patch.stopall()
        task_notification.config = None

    def test_basic_invocation(self):
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='FAILED',
            info={'id': 123, 'method': 'someMethod'},
        )
        self.make_task.assert_called_once_with(
            'taskNotification',
            ['someone@example.com', 123, 'https://koji.org'])

    def test_disable_notifications(self):
        context.opts['DisableNotifications'] = True
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='FAILED',
            info={'id': 123, 'method': 'someMethod'}
        )
        self.make_task.assert_not_called()

    def test_not_state_change(self):
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='others',
            new='something',
            info={'id': 123, 'method': 'someMethod'}
        )
        self.make_task.assert_not_called()

    def test_disallowed_state(self):
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='something',
            info={'id': 123, 'method': 'someMethod'}
        )
        self.make_task.assert_not_called()

    def test_disallowed_method(self):
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='FAILED',
            info={'id': 123, 'method': 'disallowedMethod'}
        )
        self.make_task.assert_not_called()
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='FAILED',
            info={'id': 123, 'method': 'xxxMethod'}
        )
        self.make_task.assert_not_called()
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='FAILED',
            info={'id': 123, 'method': 'Whatever'}
        )
        self.make_task.assert_not_called()

    def test_method_self(self):
        self.parser.return_value = FakeConfigParser(CONFIG2)
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='somestate',
            info={'id': 123, 'method': 'taskNotification'}
        )
        self.make_task.assert_not_called()

    def test_allow_everything(self):
        self.parser.return_value = FakeConfigParser(CONFIG2)
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='somestate',
            info={'id': 123, 'method': 'xxxMethod'}
        )
        self.make_task.assert_called_once_with(
            'taskNotification',
            ['someone@example.com', 123, 'https://koji.org'])

    def test_force_disallowed(self):
        self.parser.return_value = FakeConfigParser(CONFIG2)
        task_notification.task_notification_callback(
            'postTaskStateChange',
            attribute='state',
            new='somestate',
            info={'id': 123, 'method': 'xxxNotification'}
        )
        self.make_task.assert_not_called()

    def test_no_user_found(self):
        self.get_user.side_effect = GenericError('not found')
        with self.assertRaises(GenericError) as cm:
            task_notification.task_notification_callback(
                'postTaskStateChange',
                attribute='state',
                new='FAILED',
                info={'id': 123, 'method': 'someMethod'}
            )
        self.assertEqual(cm.exception.args[0], 'not found')
        self.make_task.assert_not_called()

    def test_disabled_owner(self):
        self.get_user.return_value = {'id': 999,
                                      'name': 'someone',
                                      'status': 1,
                                      'usertype': 0}
        with self.assertRaises(GenericError) as cm:
            task_notification.task_notification_callback(
                'postTaskStateChange',
                attribute='state',
                new='FAILED',
                info={'id': 123, 'method': 'someMethod'}
            )
        self.assertEqual(cm.exception.args[0], 'Unable to send notification to'
                                               ' disabled task#123 owner: someone')
        self.make_task.assert_not_called()

    def test_host_owner(self):
        self.get_user.return_value = {'id': 999,
                                      'name': 'somehost',
                                      'status': 0,
                                      'usertype': 1}
        with self.assertRaises(GenericError) as cm:
            task_notification.task_notification_callback(
                'postTaskStateChange',
                attribute='state',
                new='FAILED',
                info={'id': 123, 'method': 'someMethod'}
            )
        self.assertEqual(cm.exception.args[0], 'Unable to send notification to'
                                               ' host: somehost whom owns task#123')
        self.make_task.assert_not_called()