from __future__ import absolute_import, division

import os
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str,
    warn
)


def anon_handle_buildinfo(goptions, session, args):
    "[info] Print basic information about a build"
    usage = "usage: %prog buildinfo [options] <n-v-r> [<n-v-r> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--changelog", action="store_true", help="Show the changelog for the build")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify a build")
    ensure_connection(session, goptions)
    error_hit = False
    for build in args:
        if build.isdigit():
            build = int(build)
        info = session.getBuild(build)
        if info is None:
            warn("No such build: %s\n" % build)
            error_hit = True
            continue
        task = None
        if info['task_id']:
            task = session.getTaskInfo(info['task_id'], request=True)
        taglist = []
        for tag in session.listTags(build):
            taglist.append(tag['name'])
        info['arch'] = 'src'
        info['state'] = koji.BUILD_STATES[info['state']]
        print("BUILD: %(name)s-%(version)s-%(release)s [%(id)d]" % info)
        print("State: %(state)s" % info)
        if info['state'] == 'BUILDING':
            print("Reserved by: %(cg_name)s" % info)
        print("Built by: %(owner_name)s" % info)
        source = info.get('source')
        if source is not None:
            print("Source: %s" % source)
        if 'volume_name' in info:
            print("Volume: %(volume_name)s" % info)
        if task:
            print("Task: %s %s" % (task['id'], koji.taskLabel(task)))
        else:
            print("Task: none")
        print("Finished: %s" % koji.formatTimeLong(info['completion_ts']))
        maven_info = session.getMavenBuild(info['id'])
        if maven_info:
            print("Maven groupId: %s" % maven_info['group_id'])
            print("Maven artifactId: %s" % maven_info['artifact_id'])
            print("Maven version: %s" % maven_info['version'])
        win_info = session.getWinBuild(info['id'])
        if win_info:
            print("Windows build platform: %s" % win_info['platform'])
        print("Tags: %s" % ' '.join(taglist))
        if info.get('extra'):
            print("Extra: %(extra)r" % info)
        archives_seen = {}
        maven_archives = session.listArchives(buildID=info['id'], type='maven')
        if maven_archives:
            print("Maven archives:")
            for archive in maven_archives:
                archives_seen.setdefault(archive['id'], 1)
                print(os.path.join(koji.pathinfo.mavenbuild(info),
                                   koji.pathinfo.mavenfile(archive)))
        win_archives = session.listArchives(buildID=info['id'], type='win')
        if win_archives:
            print("Windows archives:")
            for archive in win_archives:
                archives_seen.setdefault(archive['id'], 1)
                print(os.path.join(koji.pathinfo.winbuild(info), koji.pathinfo.winfile(archive)))
        img_archives = session.listArchives(buildID=info['id'], type='image')
        if img_archives:
            print('Image archives:')
            for archive in img_archives:
                archives_seen.setdefault(archive['id'], 1)
                print(os.path.join(koji.pathinfo.imagebuild(info), archive['filename']))
        archive_idx = {}
        for archive in session.listArchives(buildID=info['id']):
            if archive['id'] in archives_seen:
                continue
            archive_idx.setdefault(archive['btype'], []).append(archive)
        for btype in archive_idx:
            archives = archive_idx[btype]
            print('%s Archives:' % btype.capitalize())
            for archive in archives:
                print(os.path.join(koji.pathinfo.typedir(info, btype), archive['filename']))
        rpms = session.listRPMs(buildID=info['id'])
        if rpms:
            with session.multicall() as mc:
                for rpm in rpms:
                    rpm['sigs'] = mc.queryRPMSigs(rpm['id'])
            print("RPMs:")
            for rpm in rpms:
                line = os.path.join(koji.pathinfo.build(info), koji.pathinfo.rpm(rpm))
                keys = ', '.join(sorted([x['sigkey'] for x in rpm['sigs'].result if x['sigkey']]))
                if keys:
                    line += '\tSignatures: %s' % keys
                print(line)
        if options.changelog:
            changelog = session.getChangelogEntries(info['id'])
            if changelog:
                print("Changelog:")
                print(koji.util.formatChangelog(changelog))
    if error_hit:
        error()
