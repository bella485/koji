from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_edit_target(goptions, session, args):
    "[admin] Set the name, build_tag, and/or dest_tag of an existing build target to new values"
    usage = "usage: %prog edit-target [options] <name>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--rename", help="Specify new name for target")
    parser.add_option("--build-tag", help="Specify a different build tag")
    parser.add_option("--dest-tag", help="Specify a different destination tag")

    (options, args) = parser.parse_args(args)

    if len(args) != 1:
        parser.error("Please specify a build target")
    activate_session(session, goptions)

    if not (session.hasPerm('admin') or session.hasPerm('target')):
        parser.error("This action requires target or admin privileges")

    targetInfo = session.getBuildTarget(args[0])
    if targetInfo is None:
        raise koji.GenericError("No such build target: %s" % args[0])

    targetInfo['orig_name'] = targetInfo['name']

    if options.rename:
        targetInfo['name'] = options.rename
    if options.build_tag:
        targetInfo['build_tag_name'] = options.build_tag
        chkbuildtag = session.getTag(options.build_tag)
        if not chkbuildtag:
            error("No such tag: %s" % options.build_tag)
        if not chkbuildtag.get("arches", None):
            error("Build tag has no arches: %s" % options.build_tag)
    if options.dest_tag:
        chkdesttag = session.getTag(options.dest_tag)
        if not chkdesttag:
            error("No such destination tag: %s" % options.dest_tag)
        targetInfo['dest_tag_name'] = options.dest_tag

    session.editBuildTarget(targetInfo['orig_name'], targetInfo['name'],
                            targetInfo['build_tag_name'], targetInfo['dest_tag_name'])


