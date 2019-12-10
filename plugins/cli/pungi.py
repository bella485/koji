from __future__ import absolute_import
import sys
import time
from optparse import OptionParser

import koji
from koji.plugin import export_cli
from koji_cli.lib import _, activate_session, watch_tasks

@export_cli
def handle_pungi_buildinstall(options, session, args):
    "[admin] Run a command in a buildroot"
    usage = _("usage: %prog pungi_buildinstall [options] <tag> <arch> [lorax_arguments, ...]")
    usage += _("\n(Specify the --help global option for a list of other help options)")
    parser = OptionParser(usage=usage)
    parser.disable_interspersed_args()
    parser.add_option("-p", "--package", action="append", default=[], help=_("make sure this package is in the chroot"))
    parser.add_option("-m", "--mount", action="append", default=[], help=_("mount this directory read-write in the chroot"))
    parser.add_option("-w", "--weight", type='int', help=_("set task weight"))
    parser.add_option("--chown-uid", type='int', help=_("set UID owning the output files."))
    parser.add_option("--channel-override", help=_("use a non-standard channel"))
    parser.add_option("--task-id", action="store_true", default=False,
                      help=_("Print the ID of the pungi_buildinstall task"))
    parser.add_option("--nowait", action="store_false", dest="wait", default=True, help=_("Do not wait on task"))
    parser.add_option("--watch", action="store_true", help=_("Watch task instead of printing pungi_buildinstall.log"))
    parser.add_option("--quiet", action="store_true", default=options.quiet,
                      help=_("Do not print the task information"))

    (opts, args) = parser.parse_args(args)

    if len(args) < 2:
        parser.error(_("Incorrect number of arguments"))
        assert False  # pragma: no cover

    activate_session(session, options)

    if not session.hasPerm('admin') or session.hasPerm('pungi_buildinstall'):
        parser.error(_("This action requires pungi_buildinstall or admin privileges"))

    tag = args[0]
    arch = args[1]
    lorax_args = {}
    for arg in args[2:]:
        if "=" in arg:
            k, v = arg.split("=")
            if k in lorax_args:
                if not isinstance(lorax_args[k], list):
                    lorax_args[k] = [lorax_args[k]]
                lorax_args[k].append(v)
            else:
                lorax_args[k] = v
        else:
            lorax_args[arg] = True
    try:
        kwargs = { 'channel':       opts.channel_override,
                   'packages':      opts.package,
                   'mounts':        opts.mount,
                   'weight':        opts.weight,
                   'chown_uid':     opts.chown_uid,
                   'lorax_args':    lorax_args}

        task_id = session.pungi_buildinstall(tag, arch, **kwargs)
    except koji.GenericError as e:
        if 'Invalid method' in str(e):
            print("* The pungi_buildinstall plugin appears to not be installed on the"
                  " koji hub.  Please contact the administrator.")
        raise
    if opts.task_id:
        print(task_id)

    if not opts.wait:
        return

    if opts.watch:
        session.logout()
        return watch_tasks(session, [task_id], quiet=opts.quiet,
                           poll_interval=options.poll_interval)

    try:
        while True:
            # wait for the task to finish
            if session.taskFinished(task_id):
                break
            time.sleep(options.poll_interval)
    except KeyboardInterrupt:
        # this is probably the right thing to do here
        print("User interrupt: canceling pungi_buildinstall task")
        session.cancelTask(task_id)
        raise
    sys.stdout.flush()
    info = session.getTaskInfo(task_id)
    if info is None:
        sys.exit(1)
    state = koji.TASK_STATES[info['state']]
    if state in ('FAILED', 'CANCELED'):
        sys.exit(1)
