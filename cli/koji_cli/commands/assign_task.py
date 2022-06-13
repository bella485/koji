from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_assign_task(goptions, session, args):
    "[admin] Assign a task to a host"
    usage = 'usage: %prog assign-task <task_id> <hostname>'
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option('-f', '--force', action='store_true', default=False,
                      help='force to assign a non-free task')
    (options, args) = parser.parse_args(args)

    if len(args) != 2:
        parser.error('please specify a task id and a hostname')
    else:
        task_id = int(args[0])
        hostname = args[1]

    taskinfo = session.getTaskInfo(task_id, request=False)
    if taskinfo is None:
        raise koji.GenericError("No such task: %s" % task_id)

    hostinfo = session.getHost(hostname)
    if hostinfo is None:
        raise koji.GenericError("No such host: %s" % hostname)

    force = False
    if options.force:
        force = True

    activate_session(session, goptions)
    if not session.hasPerm('admin'):
        parser.error("This action requires admin privileges")

    ret = session.assignTask(task_id, hostname, force)
    if ret:
        print('assigned task %d to host %s' % (task_id, hostname))
    else:
        print('failed to assign task %d to host %s' % (task_id, hostname))
