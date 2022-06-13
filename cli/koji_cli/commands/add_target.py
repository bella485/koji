from __future__ import absolute_import, division

from optparse import OptionParser

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_add_target(goptions, session, args):
    "[admin] Create a new build target"
    usage = "usage: %prog add-target <name> <build tag> <dest tag>"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify a target name, a build tag, and destination tag")
    elif len(args) > 3:
        parser.error("Incorrect number of arguments")
    name = args[0]
    build_tag = args[1]
    if len(args) > 2:
        dest_tag = args[2]
    else:
        # most targets have the same name as their destination
        dest_tag = name
    activate_session(session, goptions)
    if not (session.hasPerm('admin') or session.hasPerm('target')):
        parser.error("This action requires target or admin privileges")

    chkbuildtag = session.getTag(build_tag)
    chkdesttag = session.getTag(dest_tag)
    if not chkbuildtag:
        error("No such tag: %s" % build_tag)
    if not chkbuildtag.get("arches", None):
        error("Build tag has no arches: %s" % build_tag)
    if not chkdesttag:
        error("No such destination tag: %s" % dest_tag)

    session.createBuildTarget(name, build_tag, dest_tag)
