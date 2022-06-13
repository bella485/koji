from __future__ import absolute_import, division

import os
from optparse import OptionParser


import koji

from koji_cli.lib import (
    _progress_callback,
    _running_in_bg,
    activate_session,
    unique_path,
    warn,
    watch_tasks
)


def handle_dist_repo(options, session, args):
    """Create a yum repo with distribution options"""
    usage = "usage: %prog dist-repo [options] <tag> <key_id> [<key_id> ...]\n\n" \
            "In normal mode, dist-repo behaves like any other koji task.\n" \
            "Sometimes you want to limit running distRepo tasks per tag to only\n" \
            "one. For such behaviour admin (with 'tag' permission) needs to\n" \
            "modify given tag's extra field 'distrepo.cancel_others' to True'\n" \
            "via 'koji edit-tag -x distrepo.cancel_others=True'\n"
    usage += "\n(Specify the --help option for a list of other options)"
    parser = OptionParser(usage=usage)
    parser.add_option('--allow-missing-signatures', action='store_true',
                      default=False,
                      help='For RPMs not signed with a desired key, fall back to the primary copy')
    parser.add_option("-a", "--arch", action='append', default=[],
                      help="Indicate an architecture to consider. The default is all "
                           "architectures associated with the given tag. This option may "
                           "be specified multiple times.")
    parser.add_option("--with-src", action='store_true', help='Also generate a src repo')
    parser.add_option("--split-debuginfo", action='store_true', default=False,
                      help='Split debuginfo info a separate repo for each arch')
    parser.add_option('--comps', help='Include a comps file in the repodata')
    parser.add_option('--delta-rpms', metavar='REPO', default=[], action='append',
                      help='Create delta rpms. REPO can be the id of another dist repo '
                           'or the name of a tag that has a dist repo. May be specified '
                           'multiple times.')
    parser.add_option('--event', type='int', help='Use tag content at event')
    parser.add_option("--volume", help="Generate repo on given volume")
    parser.add_option('--non-latest', dest='latest', default=True,
                      action='store_false', help='Include older builds, not just the latest')
    parser.add_option('--multilib', default=None, metavar="CONFIG",
                      help='Include multilib packages in the repository using the given '
                           'config file')
    parser.add_option("--noinherit", action='store_true', default=False,
                      help='Do not consider tag inheritance')
    parser.add_option("--wait", action="store_true",
                      help="Wait for the task to complete, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Do not wait for the task to complete")
    parser.add_option('--skip-missing-signatures', action='store_true', default=False,
                      help='Skip RPMs not signed with the desired key(s)')
    parser.add_option('--zck', action='store_true', default=False,
                      help='Generate zchunk files as well as the standard repodata')
    parser.add_option('--zck-dict-dir', action='store', default=None,
                      help='Directory containing compression dictionaries for use by zchunk '
                           '(on builder)')
    parser.add_option("--write-signed-rpms", action='store_true', default=False,
                      help='Write a signed rpms for given tag')
    task_opts, args = parser.parse_args(args)
    if len(args) < 1:
        parser.error('You must provide a tag to generate the repo from')
    if len(args) < 2 and not task_opts.allow_missing_signatures:
        parser.error('Please specify one or more GPG key IDs (or --allow-missing-signatures)')
    if task_opts.allow_missing_signatures and task_opts.skip_missing_signatures:
        parser.error('allow_missing_signatures and skip_missing_signatures are mutually exclusive')
    activate_session(session, options)
    stuffdir = unique_path('cli-dist-repo')
    if task_opts.comps:
        if not os.path.exists(task_opts.comps):
            parser.error('could not find %s' % task_opts.comps)
        session.uploadWrapper(task_opts.comps, stuffdir,
                              callback=_progress_callback)
        print('')
        task_opts.comps = os.path.join(stuffdir,
                                       os.path.basename(task_opts.comps))
    old_repos = []
    if len(task_opts.delta_rpms) > 0:
        for repo in task_opts.delta_rpms:
            if repo.isdigit():
                rinfo = session.repoInfo(int(repo), strict=True)
            else:
                # get dist repo for tag
                rinfo = session.getRepo(repo, dist=True)
                if not rinfo:
                    # maybe there is an expired one
                    rinfo = session.getRepo(repo,
                                            state=koji.REPO_STATES['EXPIRED'], dist=True)
                if not rinfo:
                    parser.error("Can't find repo for tag: %s" % repo)
            old_repos.append(rinfo['id'])
    tag = args[0]
    keys = args[1:]
    taginfo = session.getTag(tag)
    if not taginfo:
        parser.error('No such tag: %s' % tag)
    allowed_arches = taginfo['arches'] or ''
    if not allowed_arches:
        for tag_inh in session.getFullInheritance(tag):
            allowed_arches = session.getTag(tag_inh['parent_id'])['arches'] or ''
            if allowed_arches:
                break
    if len(task_opts.arch) == 0:
        task_opts.arch = allowed_arches.split()
        if not task_opts.arch:
            parser.error('No arches given and no arches associated with tag')
    else:
        for a in task_opts.arch:
            if not allowed_arches:
                warn('Tag %s has an empty arch list' % taginfo['name'])
            elif a not in allowed_arches:
                warn('%s is not in the list of tag arches' % a)
    if task_opts.multilib:
        if not os.path.exists(task_opts.multilib):
            parser.error('could not find %s' % task_opts.multilib)
        if 'x86_64' in task_opts.arch and 'i686' not in task_opts.arch:
            parser.error('The multilib arch (i686) must be included')
        if 's390x' in task_opts.arch and 's390' not in task_opts.arch:
            parser.error('The multilib arch (s390) must be included')
        if 'ppc64' in task_opts.arch and 'ppc' not in task_opts.arch:
            parser.error('The multilib arch (ppc) must be included')
        session.uploadWrapper(task_opts.multilib, stuffdir,
                              callback=_progress_callback)
        task_opts.multilib = os.path.join(stuffdir,
                                          os.path.basename(task_opts.multilib))
        print('')
    if 'noarch' in task_opts.arch:
        task_opts.arch.remove('noarch')
    if task_opts.with_src and 'src' not in task_opts.arch:
        task_opts.arch.append('src')
    if not task_opts.arch:
        parser.error('No arches left.')

    opts = {
        'arch': task_opts.arch,
        'comps': task_opts.comps,
        'delta': old_repos,
        'event': task_opts.event,
        'volume': task_opts.volume,
        'inherit': not task_opts.noinherit,
        'latest': task_opts.latest,
        'multilib': task_opts.multilib,
        'split_debuginfo': task_opts.split_debuginfo,
        'skip_missing_signatures': task_opts.skip_missing_signatures,
        'allow_missing_signatures': task_opts.allow_missing_signatures,
        'zck': task_opts.zck,
        'zck_dict_dir': task_opts.zck_dict_dir,
        'write_signed_rpms': task_opts.write_signed_rpms,
    }
    task_id = session.distRepo(tag, keys, **opts)
    print("Creating dist repo for tag " + tag)
    if task_opts.wait or (task_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


_search_types = ('package', 'build', 'tag', 'target', 'user', 'host', 'rpm',
                 'maven', 'win')


