from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    _running_in_bg,
    activate_session,
    get_usage_str,
    warn,
    watch_tasks
)


def handle_regen_repo(options, session, args):
    "[admin] Force a repo to be regenerated"
    usage = "usage: %prog regen-repo [options] <tag>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--target", action="store_true",
                      help="Interpret the argument as a build target name")
    parser.add_option("--wait", action="store_true",
                      help="Wait on for regen to finish, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Don't wait on for regen to finish")
    parser.add_option("--debuginfo", action="store_true", help="Include debuginfo rpms in repo")
    parser.add_option("--source", "--src", action="store_true",
                      help="Include source rpms in each of repos")
    parser.add_option("--separate-source", "--separate-src", action="store_true",
                      help="Include source rpms in separate src repo")
    (suboptions, args) = parser.parse_args(args)
    if len(args) == 0:
        parser.error("A tag name must be specified")
    elif len(args) > 1:
        if suboptions.target:
            parser.error("Only a single target may be specified")
        else:
            parser.error("Only a single tag name may be specified")
    activate_session(session, options)
    tag = args[0]
    repo_opts = {}
    if suboptions.target:
        info = session.getBuildTarget(tag)
        if not info:
            parser.error("No such build target: %s" % tag)
        tag = info['build_tag_name']
        info = session.getTag(tag, strict=True)
    else:
        info = session.getTag(tag)
        if not info:
            parser.error("No such tag: %s" % tag)
        tag = info['name']
        targets = session.getBuildTargets(buildTagID=info['id'])
        if not targets:
            warn("%s is not a build tag" % tag)
    if not info['arches']:
        warn("Tag %s has an empty arch list" % info['name'])
    if suboptions.debuginfo:
        repo_opts['debuginfo'] = True
    if suboptions.source:
        repo_opts['src'] = True
    if suboptions.separate_source:
        repo_opts['separate_src'] = True
    task_id = session.newRepo(tag, **repo_opts)
    print("Regenerating repo for tag: %s" % tag)
    print("Created task: %d" % task_id)
    print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if suboptions.wait or (suboptions.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)
