from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    error,
    get_usage_str,
    warn,
    watch_tasks
)


def handle_restart_hosts(options, session, args):
    "[admin] Restart enabled hosts"
    usage = "usage: %prog restart-hosts [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--wait", action="store_true",
                      help="Wait on the task, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait", help="Don't wait on task")
    parser.add_option("--quiet", action="store_true",
                      help="Do not print the task information", default=options.quiet)
    parser.add_option("--force", action="store_true", help="Ignore checks and force operation")
    parser.add_option("--channel", help="Only hosts in this channel")
    parser.add_option("--arch", "-a", action="append", default=[],
                      help="Limit to hosts of this architecture (can be given multiple times)")
    parser.add_option("--timeout", metavar='N', type='int', help="Time out after N seconds")
    (my_opts, args) = parser.parse_args(args)

    if len(args) > 0:
        parser.error("restart-hosts does not accept arguments")

    activate_session(session, options)

    # check for existing restart tasks
    if not my_opts.force:
        query = {
            'method': 'restartHosts',
            'state':
                [koji.TASK_STATES[s] for s in ('FREE', 'OPEN', 'ASSIGNED')],
        }
        others = session.listTasks(query)
        if others:
            warn('Found other restartHosts tasks running.')
            warn('Task ids: %r' % [t['id'] for t in others])
            error('Use --force to run anyway')

    callopts = {}
    if my_opts.channel:
        callopts['channel'] = my_opts.channel
    if my_opts.arch:
        callopts['arches'] = my_opts.arch
    if my_opts.timeout:
        callopts['timeout'] = my_opts.timeout
    if callopts:
        task_id = session.restartHosts(options=callopts)
    else:
        # allow default case to work with older hub
        task_id = session.restartHosts()
    if my_opts.wait or (my_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=my_opts.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)
