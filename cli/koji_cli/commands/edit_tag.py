from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    activate_session,
    arg_filter,
    get_usage_str
)


def handle_edit_tag(goptions, session, args):
    "[admin] Alter tag information"
    usage = "usage: %prog edit-tag [options] <name>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arches", help="Specify arches")
    parser.add_option("--perm", help="Specify permission requirement")
    parser.add_option("--no-perm", action="store_true", help="Remove permission requirement")
    parser.add_option("--lock", action="store_true", help="Lock the tag")
    parser.add_option("--unlock", action="store_true", help="Unlock the tag")
    parser.add_option("--rename", help="Rename the tag")
    parser.add_option("--maven-support", action="store_true",
                      help="Enable creation of Maven repos for this tag")
    parser.add_option("--no-maven-support", action="store_true",
                      help="Disable creation of Maven repos for this tag")
    parser.add_option("--include-all", action="store_true",
                      help="Include all packages in this tag when generating Maven repos")
    parser.add_option("--no-include-all", action="store_true",
                      help="Do not include all packages in this tag when generating Maven repos")
    parser.add_option("-x", "--extra", action="append", default=[], metavar="key=value",
                      help="Set tag extra option. JSON-encoded or simple value")
    parser.add_option("-r", "--remove-extra", action="append", default=[], metavar="key",
                      help="Remove tag extra option")
    parser.add_option("-b", "--block-extra", action="append", default=[], metavar="key",
                      help="Block inherited tag extra option")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Please specify a name for the tag")
    activate_session(session, goptions)
    tag = args[0]
    opts = {}
    if options.arches:
        opts['arches'] = koji.parse_arches(options.arches)
    if options.no_perm:
        opts['perm'] = None
    elif options.perm:
        opts['perm'] = options.perm
    if options.unlock:
        opts['locked'] = False
    if options.lock:
        opts['locked'] = True
    if options.rename:
        opts['name'] = options.rename
    if options.maven_support:
        opts['maven_support'] = True
    if options.no_maven_support:
        opts['maven_support'] = False
    if options.include_all:
        opts['maven_include_all'] = True
    if options.no_include_all:
        opts['maven_include_all'] = False
    if options.extra:
        extra = {}
        for xopt in options.extra:
            key, value = xopt.split('=', 1)
            if key in extra:
                parser.error("Duplicate extra key: %s" % key)
            extra[key] = arg_filter(value, parse_json=True)
        opts['extra'] = extra
    opts['remove_extra'] = options.remove_extra
    opts['block_extra'] = options.block_extra
    # XXX change callname
    session.editTag2(tag, **opts)
