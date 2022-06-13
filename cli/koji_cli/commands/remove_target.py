from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_remove_target(goptions, session, args):
    "[admin] Remove a build target"
    usage = "usage: %prog remove-target [options] <name>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)

    if len(args) != 1:
        parser.error("Please specify a build target to remove")
    activate_session(session, goptions)

    if not (session.hasPerm('admin') or session.hasPerm('target')):
        parser.error("This action requires target or admin privileges")

    target = args[0]
    target_info = session.getBuildTarget(target)
    if not target_info:
        error("No such build target: %s" % target)

    session.deleteBuildTarget(target_info['id'])
