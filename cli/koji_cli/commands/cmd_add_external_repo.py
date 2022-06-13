from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_add_external_repo(goptions, session, args):
    "[admin] Create an external repo and/or add one to a tag"
    usage = "usage: %prog add-external-repo [options] <name> [<url>]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-t", "--tag", action="append", metavar="TAG",
                      help="Also add repo to tag. Use tag::N to set priority")
    parser.add_option("-p", "--priority", type='int',
                      help="Set priority (when adding to tag)")
    parser.add_option("-m", "--mode", help="Set merge mode")
    parser.add_option("-a", "--arches", metavar="ARCH1,ARCH2, ...",
                      help="Use only subset of arches from given repo")
    (options, args) = parser.parse_args(args)
    activate_session(session, goptions)
    if options.mode:
        if options.mode not in koji.REPO_MERGE_MODES:
            parser.error('Invalid mode: %s' % options.mode)
        if not options.tag:
            parser.error('The --mode option can only be used with --tag')
    if len(args) == 1:
        name = args[0]
        rinfo = session.getExternalRepo(name, strict=True)
        if not options.tag:
            parser.error("A url is required to create an external repo entry")
    elif len(args) == 2:
        name, url = args
        rinfo = session.createExternalRepo(name, url)
        print("Created external repo %(id)i" % rinfo)
    else:
        parser.error("Incorrect number of arguments")
    if options.tag:
        for tagpri in options.tag:
            tag, priority = _parse_tagpri(tagpri)
            if priority is None:
                if options.priority is not None:
                    priority = options.priority
                else:
                    priority = _pick_external_repo_priority(session, tag)
            callopts = {}
            if options.mode:
                callopts['merge_mode'] = options.mode
            if options.arches:
                callopts['arches'] = options.arches
            session.addExternalRepoToTag(tag, rinfo['name'], priority, **callopts)
            print("Added external repo %s to tag %s (priority %i)"
                  % (rinfo['name'], tag, priority))


