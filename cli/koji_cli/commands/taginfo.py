from __future__ import absolute_import, division

import pprint
import time
from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    format_inheritance_flags,
    get_usage_str
)


def anon_handle_taginfo(goptions, session, args):
    "[info] Print basic information about a tag"
    usage = "usage: %prog taginfo [options] <tag> [<tag> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--event", type='int', metavar="EVENT#", help="query at event")
    parser.add_option("--ts", type='int', metavar="TIMESTAMP",
                      help="query at last event before timestamp")
    parser.add_option("--repo", type='int', metavar="REPO#", help="query at event for a repo")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify a tag")
    ensure_connection(session, goptions)
    event = koji.util.eventFromOpts(session, options)
    event_opts = {}
    if event:
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        print("Querying at event %(id)i (%(timestr)s)" % event)
        event_opts['event'] = event['id']
    perms = dict([(p['id'], p['name']) for p in session.getAllPerms()])

    tags = []
    for tag in args:
        info = session.getBuildConfig(tag, **event_opts)
        if info is None:
            try:
                info = session.getBuildConfig(int(tag), **event_opts)
            except ValueError:
                info = None
            if info is None:
                parser.error('No such tag: %s' % tag)
        tags.append(info)

    for n, info in enumerate(tags):
        if n > 0:
            print('')
        print("Tag: %(name)s [%(id)d]" % info)
        print("Arches: %(arches)s" % info)
        group_list = sorted([x['name'] for x in session.getTagGroups(info['id'], **event_opts)])
        print("Groups: " + ', '.join(group_list))
        if info.get('locked'):
            print('LOCKED')
        if info.get('perm_id') is not None:
            perm_id = info['perm_id']
            print("Required permission: %r" % perms.get(perm_id, perm_id))
        if session.mavenEnabled():
            print("Maven support?: %s" % (info['maven_support'] and 'yes' or 'no'))
            print("Include all Maven archives?: %s" %
                  (info['maven_include_all'] and 'yes' or 'no'))
        if 'extra' in info:
            print("Tag options:")
            for key in sorted(info['extra'].keys()):
                line = "  %s : %s" % (key, pprint.pformat(info['extra'][key]))
                if key in info.get('config_inheritance', {}).get('extra', []):
                    line = "%-30s [%s]" % (line, info['config_inheritance']['extra'][key]['name'])
                print(line)
        dest_targets = session.getBuildTargets(destTagID=info['id'], **event_opts)
        build_targets = session.getBuildTargets(buildTagID=info['id'], **event_opts)
        repos = {}
        if not event:
            for target in dest_targets + build_targets:
                if target['build_tag'] not in repos:
                    repo = session.getRepo(target['build_tag'])
                    if repo is None:
                        repos[target['build_tag']] = "no active repo"
                    else:
                        repos[target['build_tag']] = "repo#%(id)i: %(creation_time)s" % repo
        if dest_targets:
            print("Targets that build into this tag:")
            for target in dest_targets:
                if event:
                    print("  %s (%s)" % (target['name'], target['build_tag_name']))
                else:
                    print("  %s (%s, %s)" %
                          (target['name'], target['build_tag_name'], repos[target['build_tag']]))
        if build_targets:
            print("This tag is a buildroot for one or more targets")
            if not event:
                print("Current repo: %s" % repos[info['id']])
            print("Targets that build from this tag:")
            for target in build_targets:
                print("  %s" % target['name'])
        external_repos = session.getTagExternalRepos(tag_info=info['id'], **event_opts)
        if external_repos:
            print("External repos:")
            for rinfo in external_repos:
                if 'arches' not in rinfo:
                    # older hubs will not return this field
                    rinfo['arches'] = '-'
                elif not rinfo['arches']:
                    rinfo['arches'] = 'inherited from tag'
                    # TODO else intersection of arches?
                print("  %(priority)3i %(external_repo_name)s "
                      "(%(url)s, merge mode: %(merge_mode)s), arches: %(arches)s" % rinfo)
        print("Inheritance:")
        for parent in session.getInheritanceData(info['id'], **event_opts):
            parent['flags'] = format_inheritance_flags(parent)
            print("  %(priority)-4d %(flags)s %(name)s [%(parent_id)s]" % parent)
            if parent['maxdepth'] is not None:
                print("    maxdepth: %(maxdepth)s" % parent)
            if parent['pkg_filter']:
                print("    package filter: %(pkg_filter)s" % parent)
