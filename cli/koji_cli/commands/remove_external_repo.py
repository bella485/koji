from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str,
    warn
)


def handle_remove_external_repo(goptions, session, args):
    "[admin] Remove an external repo from a tag or tags, or remove entirely"
    usage = "usage: %prog remove-external-repo <repo> [<tag> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--alltags", action="store_true", help="Remove from all tags")
    parser.add_option("--force", action='store_true', help="Force action")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Incorrect number of arguments")
    activate_session(session, goptions)
    repo = args[0]
    tags = args[1:]
    delete = not bool(tags)
    data = session.getTagExternalRepos(repo_info=repo)
    current_tags = [d['tag_name'] for d in data]
    if options.alltags:
        delete = False
        if tags:
            parser.error("Do not specify tags when using --alltags")
        if not current_tags:
            if options.force:
                delete = True
            else:
                warn("External repo %s not associated with any tags" % repo)
                return 0
        tags = current_tags
    if delete:
        # removing entirely
        if current_tags and not options.force:
            warn("Error: external repo %s used by tag(s): %s" % (repo, ', '.join(current_tags)))
            error("Use --force to remove anyway")
        session.deleteExternalRepo(args[0])
    else:
        for tag in tags:
            if tag not in current_tags:
                print("External repo %s not associated with tag %s" % (repo, tag))
                continue
            session.removeExternalRepoFromTag(tag, repo)
