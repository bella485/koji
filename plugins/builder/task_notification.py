import smtplib
import sys

import koji
import koji.tasks as tasks

__all__ = ('TaskNotificationTask',)

class TaskNotificationTask(tasks.BaseTaskHandler):
    Methods = ['taskNotification']

    _taskWeight = 0.1

    # XXX externalize these templates somewhere
    subject_templ = """Task: #%(id)d Status: %(state_str)s Owner: %(owner_name)s"""
    message_templ = \
"""From: %(from_addr)s\r
Subject: %(subject)s\r
To: %(to_addrs)s\r
X-Koji-Task: %(id)s\r
X-Koji-Owner: %(owner_name)s\r
X-Koji-Status: %(state_str)s\r
X-Koji-Parent: %(parent)s\r
X-Koji-Method: %(method)s\r
\r
Task: %(id)s\r
Status: %(state_str)s\r
Owner: %(owner_name)s\r
Host: %(host_name)s\r
Method: %(method)s\r
Parent: %(parent)s\r
Arch: %(arch)s\r
Label: %(label)s\r
Created: %(create_time)s\r
Started: %(start_time)s\r
Finished: %(completion_time)s\r
%(failure)s\r
Task Info: %(weburl)s/taskinfo?taskID=%(id)i\r
"""

    def _get_notification_info(self, task_id, recipient, weburl):
        taskinfo = self.session.getTaskInfo(task_id, request=True)

        if not taskinfo:
            # invalid task_id
            raise koji.GenericError('Cannot find task#%i' % task_id)

        if taskinfo['host_id']:
            hostinfo = self.session.getHost(taskinfo['host_id'])
        else:
            hostinfo = None

        if taskinfo['owner']:
            userinfo = self.session.getUser(taskinfo['owner'], strict=True)
            taskinfo['owner_name'] = userinfo['name']
        else:
            taskinfo['owner_name'] = None

        result = None
        try:
            result = self.session.getTaskResult(task_id)
        except:
            excClass, result = sys.exc_info()[:2]
            if hasattr(result, 'faultString'):
                result = result.faultString
            else:
                result = '%s: %s' % (excClass.__name__, result)
            result = result.strip()
            # clear the exception, since we're just using
            # it for display purposes
            sys.exc_clear()
        if not result:
            result = 'Unknown'
        taskinfo['result'] = result

        noti_info = taskinfo.copy()
        noti_info['host_name'] = hostinfo and hostinfo['name'] or None
        noti_info['state_str'] = koji.TASK_STATES[taskinfo['state']]

        cancel_info = ''
        failure_info = ''
        if taskinfo['state'] == koji.TASK_STATES['CANCELED']:
            # The owner of the buildNotification task is the one
            # who canceled the task, it turns out.
            this_task = self.session.getTaskInfo(self.id)
            if this_task['owner']:
                canceler = self.session.getUser(this_task['owner'], strict=True)
                cancel_info = "\r\nCanceled by: %s\r\n" % canceler['name']
        elif taskinfo['state'] == koji.TASK_STATES['FAILED']:
            failure_data = taskinfo['result']
            failed_host = '%s (%s)' % (noti_info['host_name'], noti_info['arch'])
            failure_info = "\r\nTask#%s failed on %s:\r\n  %s" % (task_id, failed_host, failure_data)

        noti_info['failure'] = failure_info or cancel_info or '\r\n'

        noti_info['from_addr'] = self.options.from_addr
        noti_info['to_addrs'] = recipient
        noti_info['subject'] = self.subject_templ % noti_info
        noti_info['weburl'] = weburl
        return noti_info

    def handler(self, recipient, task_id, weburl):
        noti_info = self._get_notification_info(task_id, recipient, weburl)

        message = self.message_templ % noti_info
        # ensure message is in UTF-8
        message = koji.fixEncoding(message)

        server = smtplib.SMTP(self.options.smtphost)
        # server.set_debuglevel(True)
        self.logger.debug("send notification for task #%i to %s, message:\n%s" % (task_id, recipient, message))
        server.sendmail(noti_info['from_addr'], recipient, message)
        server.quit()

        return 'sent notification of task #%i to: %s' % (task_id, recipient)

