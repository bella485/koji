from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_edit_external_repo(goptions, session, args):
    "[admin] Edit data for an external repo"
    usage = "usage: %prog edit-external-repo [options] <name>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--url", help="Change the url")
    parser.add_option("--name", help="Change the name")
    parser.add_option("-t", "--tag", metavar="TAG", help="Edit the repo properties for the tag.")
    parser.add_option("-p", "--priority", metavar="PRIORITY", type='int',
                      help="Edit the priority of the repo for the tag specified by --tag.")
    parser.add_option("-m", "--mode", metavar="MODE",
                      help="Edit the merge mode of the repo for the tag specified by --tag. "
                           "Options: %s." % ", ".join(koji.REPO_MERGE_MODES))
    parser.add_option("-a", "--arches", metavar="ARCH1,ARCH2, ...",
                      help="Use only subset of arches from given repo")
    (options, args) = parser.parse_args(args)
    if len(args) != 1:
        parser.error("Incorrect number of arguments")
    repo_opts = {}
    if options.url:
        repo_opts['url'] = options.url
    if options.name:
        repo_opts['name'] = options.name
    tag_repo_opts = {}
    if options.tag:
        if options.priority is not None:
            tag_repo_opts['priority'] = options.priority
        if options.mode:
            tag_repo_opts['merge_mode'] = options.mode
        if options.arches is not None:
            tag_repo_opts['arches'] = options.arches
        if not tag_repo_opts:
            parser.error("At least, one of priority and merge mode should be specified")
        tag_repo_opts['tag_info'] = options.tag
        tag_repo_opts['repo_info'] = args[0]
    else:
        for k in ('priority', 'mode', 'arches'):
            if getattr(options, k) is not None:
                parser.error("If %s is specified, --tag must be specified as well" % k)

    if not (repo_opts or tag_repo_opts):
        parser.error("No changes specified")

    activate_session(session, goptions)
    if repo_opts:
        session.editExternalRepo(args[0], **repo_opts)
    if tag_repo_opts:
        session.editTagExternalRepo(**tag_repo_opts)


