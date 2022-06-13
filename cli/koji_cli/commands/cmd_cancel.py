from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str,
    warn
)


def handle_cancel(goptions, session, args):
    "[build] Cancel tasks and/or builds"
    usage = "usage: %prog cancel [options] <task_id|build> [<task_id|build> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--justone", action="store_true", help="Do not cancel subtasks")
    parser.add_option("--full", action="store_true", help="Full cancellation (admin only)")
    parser.add_option("--force", action="store_true", help="Allow subtasks with --full")
    (options, args) = parser.parse_args(args)
    if len(args) == 0:
        parser.error("You must specify at least one task id or build")
    activate_session(session, goptions)
    tlist = []
    blist = []
    for arg in args:
        try:
            tlist.append(int(arg))
        except ValueError:
            try:
                koji.parse_NVR(arg)
                blist.append(arg)
            except koji.GenericError:
                parser.error("please specify only task ids (integer) or builds (n-v-r)")

    results = []
    with session.multicall(strict=False, batch=100) as m:
        if tlist:
            opts = {}
            remote_fn = m.cancelTask
            if options.justone:
                opts['recurse'] = False
            elif options.full:
                remote_fn = m.cancelTaskFull
                if options.force:
                    opts['strict'] = False
            for task_id in tlist:
                results.append(remote_fn(task_id, **opts))
        for build in blist:
            results.append(m.cancelBuild(build))

    err = False
    for r in results:
        if isinstance(r.result, dict):
            warn(r.result['faultString'])
            err = True
    if err:
        return 1


