from __future__ import absolute_import
import os
import mock
import unittest
import xmlrpclib
from mock import call

import koji
from . import load_plugin
task_notification = load_plugin.load_plugin('builder', 'task_notification')

taskinfo = {'id': 111,
            'host_id': 2,
            'owner': 222,
            'state': 0,
            'parent': None,
            'method': 'someMethod',
            'arch': 'someArch',
            'label': None,
            'create_time': '2017-01-01 00:00:00.121313',
            'start_time': '2017-02-01 00:00:00.121313',
            'completion_time': '2017-01-01 00:00:00.121313'}

hostinfo = {'id': 2, 'name': 'task.host.com'}
userinfo = {'id': 222, 'name': 'somebody'}
taskresult = 'task result'


class TestTaskNotification(unittest.TestCase):
    def setUp(self):
        self.original_timezone = os.environ.get('TZ')
        os.environ['TZ'] = 'US/Eastern'
        self.session = mock.MagicMock()
        self.session.getTaskInfo.return_value = taskinfo
        self.session.getHost.return_value = hostinfo
        self.session.getUser.return_value = userinfo
        self.session.getTaskResult.return_value = taskresult
        self.smtpClass = mock.patch("smtplib.SMTP").start()
        self.smtp_server = self.smtpClass.return_value
        options = mock.MagicMock()
        options.from_addr = 'koji@example.com'
        self.task = task_notification.TaskNotificationTask(666, 'taskNotification', {}, self.session, options)

    def tearDown(self):
        if self.original_timezone is None:
            del os.environ['TZ']
        else:
            os.environ['TZ'] = self.original_timezone
        mock.patch.stopall()

    def reset_mock(self):
        self.session.reset_mock()
        self.smtpClass.reset_mock()

    def test_task_notification_canceled(self):
        ti = taskinfo.copy()
        ti['state'] = 3  # canceled
        self.session.getTaskInfo.side_effect = [ti,
                                                {'id': 666,
                                                 'owner': 333}]
        self.session.getUser.side_effect = [userinfo, {'id': 666, 'name': 'notitaskowner'}]

        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.assertEqual(self.session.getTaskInfo.mock_calls, [call(111, request=True), call(666)])
        self.session.getHost.assert_called_once_with(2)
        self.assertEqual(self.session.getUser.mock_calls, [call(222, strict=True), call(333, strict=True)])
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: CANCELED Owner: somebody\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: somebody\r\n'
                                                          'X-Koji-Status: CANCELED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: CANCELED\r\n'
                                                          'Owner: somebody\r\n'
                                                          'Host: task.host.com\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n'
                                                          'Canceled by: notitaskowner\r\n\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

    def test_task_notification_failed(self):
        ti = taskinfo.copy()
        ti['state'] = 5  # failed
        self.session.getTaskInfo.return_value = ti

        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_called_once_with(2)
        self.session.getUser.assert_called_once_with(222, strict=True)
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: FAILED Owner: somebody\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: somebody\r\n'
                                                          'X-Koji-Status: FAILED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: FAILED\r\n'
                                                          'Owner: somebody\r\n'
                                                          'Host: task.host.com\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n'
                                                          'Task#111 failed on task.host.com (someArch):\r\n'
                                                          '  task result\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

        self.reset_mock()
        self.session.getTaskResult.return_value = None
        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_called_once_with(2)
        self.session.getUser.assert_called_once_with(222, strict=True)
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: FAILED Owner: somebody\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: somebody\r\n'
                                                          'X-Koji-Status: FAILED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: FAILED\r\n'
                                                          'Owner: somebody\r\n'
                                                          'Host: task.host.com\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n'
                                                          'Task#111 failed on task.host.com (someArch):\r\n'
                                                          '  Unknown\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

        self.reset_mock()
        self.session.getTaskResult.side_effect = xmlrpclib.Fault(1231, 'xmlrpc fault')
        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_called_once_with(2)
        self.session.getUser.assert_called_once_with(222, strict=True)
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: FAILED Owner: somebody\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: somebody\r\n'
                                                          'X-Koji-Status: FAILED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: FAILED\r\n'
                                                          'Owner: somebody\r\n'
                                                          'Host: task.host.com\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n'
                                                          'Task#111 failed on task.host.com (someArch):\r\n'
                                                          '  xmlrpc fault\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

        self.reset_mock()
        self.session.getTaskResult.side_effect = koji.GenericError('koji generic error')
        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_called_once_with(2)
        self.session.getUser.assert_called_once_with(222, strict=True)
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: FAILED Owner: somebody\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: somebody\r\n'
                                                          'X-Koji-Status: FAILED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: FAILED\r\n'
                                                          'Owner: somebody\r\n'
                                                          'Host: task.host.com\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n'
                                                          'Task#111 failed on task.host.com (someArch):\r\n'
                                                          '  GenericError: koji generic error\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

    def test_task_notification_other_status(self):
        ti = taskinfo.copy()
        ti['state'] = 2  # closed
        self.session.getTaskInfo.return_value = ti

        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_called_once_with(2)
        self.session.getUser.assert_called_once_with(222, strict=True)
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: CLOSED Owner: somebody\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: somebody\r\n'
                                                          'X-Koji-Status: CLOSED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: CLOSED\r\n'
                                                          'Owner: somebody\r\n'
                                                          'Host: task.host.com\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

    def test_task_notification_no_host_user(self):
        ti = taskinfo.copy()
        ti['state'] = 2  # closed
        ti['host_id'] = None
        ti['owner'] = None
        self.session.getTaskInfo.return_value = ti
        self.session.getHost.return_value = None
        self.session.getUser.return_value = None

        rv = self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(rv, 'sent notification of task #111 to: someone@example.com')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_not_called()
        self.session.getUser.assert_not_called()
        self.session.getTaskResult.assert_called_once_with(111)
        self.smtp_server.sendmail.assert_called_once_with('koji@example.com', 'someone@example.com',
                                                          'From: koji@example.com\r\n'
                                                          'Subject: Task: #111 Status: CLOSED Owner: None\r\n'
                                                          'To: someone@example.com\r\n'
                                                          'X-Koji-Task: 111\r\n'
                                                          'X-Koji-Owner: None\r\n'
                                                          'X-Koji-Status: CLOSED\r\n'
                                                          'X-Koji-Parent: None\r\n'
                                                          'X-Koji-Method: someMethod\r\n\r\n'
                                                          'Task: 111\r\n'
                                                          'Status: CLOSED\r\n'
                                                          'Owner: None\r\n'
                                                          'Host: None\r\n'
                                                          'Method: someMethod\r\n'
                                                          'Parent: None\r\n'
                                                          'Arch: someArch\r\n'
                                                          'Label: None\r\n'
                                                          'Created: Sun, 01 Jan 2017 00:00:00 EST\r\n'
                                                          'Started: Wed, 01 Feb 2017 00:00:00 EST\r\n'
                                                          'Finished: Sun, 01 Jan 2017 00:00:00 EST\r\n\r\n\r\n'
                                                          'Task Info: https://kojiurl.com/taskinfo?taskID=111\r\n')

    def test_task_notification_no_taskinfo(self):
        self.session.getTaskInfo.return_value = None
        with self.assertRaises(koji.GenericError) as cm:
            self.task.handler('someone@example.com', 111, 'https://kojiurl.com')
        self.assertEqual(cm.exception.args[0], 'Cannot find task#111')
        self.session.getTaskInfo.assert_called_once_with(111, request=True)
        self.session.getHost.assert_not_called()
        self.session.getUser.assert_not_called()
        self.session.getTaskResult.assert_not_called()
        self.smtp_server.sendmail.assert_not_called()
