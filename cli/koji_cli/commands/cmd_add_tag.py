from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    arg_filter,
    get_usage_str
)


def handle_add_tag(goptions, session, args):
    "[admin] Add a new tag to the database"
    usage = "usage: %prog add-tag [options] <name>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--parent", help="Set a parent tag with priority 0")
    parser.add_option("--arches", help="Specify arches")
    parser.add_option("--maven-support", action="store_true",
                      help="Enable creation of Maven repos for this tag")
    parser.add_option("--include-all", action="store_true",
                      help="Include all packages in this tag when generating Maven repos")
    parser.add_option("-x", "--extra", action="append", default=[], metavar="key=value",
                      help="Set tag extra option")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Please specify a name for the tag")
    activate_session(session, goptions)
    if not (session.hasPerm('admin') or session.hasPerm('tag')):
        parser.error("This action requires tag or admin privileges")
    opts = {}
    if options.parent:
        opts['parent'] = options.parent
    if options.arches:
        opts['arches'] = koji.parse_arches(options.arches)
    if options.maven_support:
        opts['maven_support'] = True
    if options.include_all:
        opts['maven_include_all'] = True
    if options.extra:
        extra = {}
        for xopt in options.extra:
            key, value = xopt.split('=', 1)
            value = arg_filter(value)
            extra[key] = value
        opts['extra'] = extra
    session.createTag(args[0], **opts)


