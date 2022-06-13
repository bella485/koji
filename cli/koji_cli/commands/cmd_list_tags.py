from __future__ import absolute_import, division

import itertools
import pprint
import sys
import time
from datetime import datetime
from dateutil.tz import tzutc
from optparse import OptionParser


import koji
from koji.util import to_list

from koji_cli.lib import (
    ensure_connection,
    get_usage_str,
    warn
)


def anon_handle_list_tags(goptions, session, args):
    "[info] Print the list of tags"
    usage = "usage: %prog list-tags [options] [pattern]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--show-id", action="store_true", help="Show tag ids")
    parser.add_option("--verbose", action="store_true", help="Show more information")
    parser.add_option("--unlocked", action="store_true", help="Only show unlocked tags")
    parser.add_option("--build", help="Show tags associated with a build")
    parser.add_option("--package", help="Show tags associated with a package")
    (options, patterns) = parser.parse_args(args)
    ensure_connection(session, goptions)

    pkginfo = {}
    buildinfo = {}

    if options.package:
        pkginfo = session.getPackage(options.package)
        if not pkginfo:
            parser.error("No such package: %s" % options.package)

    if options.build:
        buildinfo = session.getBuild(options.build)
        if not buildinfo:
            parser.error("No such build: %s" % options.build)

    if not patterns:
        # list everything if no pattern is supplied
        tags = session.listTags(build=buildinfo.get('id', None),
                                package=pkginfo.get('id', None))
    else:
        # The hub may not support the pattern option. We try with that first
        # and fall back to the old way.
        fallback = False
        try:
            tags = []
            with session.multicall(strict=True) as m:
                for pattern in patterns:
                    tags.append(m.listTags(build=buildinfo.get('id', None),
                                           package=pkginfo.get('id', None),
                                           pattern=pattern))
            tags = list(itertools.chain(*[t.result for t in tags]))
        except koji.ParameterError:
            fallback = True
        if fallback:
            # without the pattern option, we have to filter client side
            tags = session.listTags(buildinfo.get('id', None), pkginfo.get('id', None))
            tags = [t for t in tags if koji.util.multi_fnmatch(t['name'], patterns)]

    tags.sort(key=lambda x: x['name'])
    # if options.verbose:
    #    fmt = "%(name)s [%(id)i] %(perm)s %(locked)s %(arches)s"
    if options.show_id:
        fmt = "%(name)s [%(id)i]"
    else:
        fmt = "%(name)s"
    for tag in tags:
        if options.unlocked:
            if tag['locked'] or tag['perm']:
                continue
        if not options.verbose:
            print(fmt % tag)
        else:
            sys.stdout.write(fmt % tag)
            if tag['locked']:
                sys.stdout.write(' [LOCKED]')
            if tag['perm']:
                sys.stdout.write(' [%(perm)s perm required]' % tag)
            print('')


def _print_histline(entry, **kwargs):
    options = kwargs['options']
    event_id, table, create, x = entry
    who = None
    edit = x.get('.related')
    if edit:
        del x['.related']
        bad_edit = None
        if len(edit) != 1:
            bad_edit = "%i elements" % (len(edit) + 1)
        other = edit[0]
        # check edit for sanity
        if create or not other[2]:
            bad_edit = "out of order"
        if event_id != other[0]:
            bad_edit = "non-matching"
        if bad_edit:
            warn("Unusual edit at event %i in table %s (%s)" % (event_id, table, bad_edit))
            # we'll simply treat them as separate events
            pprint.pprint(entry)
            pprint.pprint(edit)
            _print_histline(entry, **kwargs)
            for data in edit:
                _print_histline(entry, **kwargs)
            return
    if create:
        ts = x['create_ts']
        if 'creator_name' in x:
            who = "by %(creator_name)s"
    else:
        ts = x['revoke_ts']
        if 'revoker_name' in x:
            who = "by %(revoker_name)s"
    if table == 'tag_listing':
        if edit:
            fmt = "%(name)s-%(version)s-%(release)s re-tagged into %(tag.name)s"
        elif create:
            fmt = "%(name)s-%(version)s-%(release)s tagged into %(tag.name)s"
        else:
            fmt = "%(name)s-%(version)s-%(release)s untagged from %(tag.name)s"
    elif table == 'user_perms':
        if edit:
            fmt = "permission %(permission.name)s re-granted to %(user.name)s"
        elif create:
            fmt = "permission %(permission.name)s granted to %(user.name)s"
        else:
            fmt = "permission %(permission.name)s revoked for %(user.name)s"
    elif table == 'user_groups':
        if edit:
            fmt = "user %(user.name)s re-added to group %(group.name)s"
        elif create:
            fmt = "user %(user.name)s added to group %(group.name)s"
        else:
            fmt = "user %(user.name)s removed from group %(group.name)s"
    elif table == 'cg_users':
        if edit:
            fmt = "user %(user.name)s re-added to content generator %(content_generator.name)s"
        elif create:
            fmt = "user %(user.name)s added to content generator %(content_generator.name)s"
        else:
            fmt = "user %(user.name)s removed from content generator %(content_generator.name)s"
    elif table == 'tag_packages':
        if edit:
            fmt = "package list entry for %(package.name)s in %(tag.name)s updated"
        elif create:
            fmt = "package list entry created: %(package.name)s in %(tag.name)s"
        else:
            fmt = "package list entry revoked: %(package.name)s in %(tag.name)s"
    elif table == 'tag_package_owners':
        if edit:
            fmt = "package owner changed for %(package.name)s in %(tag.name)s"
        elif create:
            fmt = "package owner %(owner.name)s set for %(package.name)s in %(tag.name)s"
        else:
            fmt = "package owner %(owner.name)s revoked for %(package.name)s in %(tag.name)s"
    elif table == 'tag_inheritance':
        if edit:
            fmt = "inheritance line %(tag.name)s->%(parent.name)s updated"
        elif create:
            fmt = "inheritance line %(tag.name)s->%(parent.name)s added"
        else:
            fmt = "inheritance line %(tag.name)s->%(parent.name)s removed"
    elif table == 'tag_config':
        if edit:
            fmt = "tag configuration for %(tag.name)s altered"
        elif create:
            fmt = "new tag: %(tag.name)s"
        else:
            fmt = "tag deleted: %(tag.name)s"
    elif table == 'tag_extra':
        if edit:
            fmt = "tag option %(key)s for tag %(tag.name)s altered"
        elif create:
            fmt = "added tag option %(key)s for tag %(tag.name)s"
        else:
            fmt = "tag option %(key)s removed for %(tag.name)s"
    elif table == 'host_config':
        if edit:
            fmt = "host configuration for %(host.name)s altered"
        elif create:
            fmt = "new host: %(host.name)s"
        else:
            fmt = "host deleted: %(host.name)s"
    elif table == 'host_channels':
        if create:
            fmt = "host %(host.name)s added to channel %(channels.name)s"
        else:
            fmt = "host %(host.name)s removed from channel %(channels.name)s"
    elif table == 'build_target_config':
        if edit:
            fmt = "build target configuration for %(build_target.name)s updated"
        elif create:
            fmt = "new build target: %(build_target.name)s"
        else:
            fmt = "build target deleted: %(build_target.name)s"
    elif table == 'external_repo_config':
        if edit:
            fmt = "external repo configuration for %(external_repo.name)s altered"
        elif create:
            fmt = "new external repo: %(external_repo.name)s"
        else:
            fmt = "external repo deleted: %(external_repo.name)s"
    elif table == 'tag_external_repos':
        if edit:
            fmt = "external repo entry for %(external_repo.name)s in tag %(tag.name)s updated"
        elif create:
            fmt = "external repo entry for %(external_repo.name)s added to tag %(tag.name)s"
        else:
            fmt = "external repo entry for %(external_repo.name)s removed from tag %(tag.name)s"
    elif table == 'group_config':
        if edit:
            fmt = "group %(group.name)s configuration for tag %(tag.name)s updated"
        elif create:
            fmt = "group %(group.name)s added to tag %(tag.name)s"
        else:
            fmt = "group %(group.name)s removed from tag %(tag.name)s"
    elif table == 'group_req_listing':
        if edit:
            fmt = "group dependency %(group.name)s->%(req.name)s updated in tag %(tag.name)s"
        elif create:
            fmt = "group dependency %(group.name)s->%(req.name)s added in tag %(tag.name)s"
        else:
            fmt = "group dependency %(group.name)s->%(req.name)s dropped from tag %(tag.name)s"
    elif table == 'group_package_listing':
        if edit:
            fmt = "package entry %(package)s in group %(group.name)s, tag %(tag.name)s updated"
        elif create:
            fmt = "package %(package)s added to group %(group.name)s in tag %(tag.name)s"
        else:
            fmt = "package %(package)s removed from group %(group.name)s in tag %(tag.name)s"
    else:
        if edit:
            fmt = "%s entry updated" % table
        elif create:
            fmt = "%s entry created" % table
        else:
            fmt = "%s entry revoked" % table
    if options.utc:
        time_str = time.asctime(datetime.fromtimestamp(ts, tzutc()).timetuple())
    else:
        time_str = time.asctime(time.localtime(ts))

    parts = [time_str, fmt % x]
    if options.events or options.verbose:
        parts.insert(1, "(eid %i)" % event_id)
    if who:
        parts.append(who % x)
    if create and x['active']:
        parts.append("[still active]")
    print(' '.join(parts))
    hidden_fields = ['active', 'create_event', 'revoke_event', 'creator_id', 'revoker_id',
                     'creator_name', 'revoker_name', 'create_ts', 'revoke_ts']

    def get_nkey(key):
        if key == 'perm_id':
            return 'permission.name'
        elif key.endswith('_id'):
            return '%s.name' % key[:-3]
        else:
            return '%s.name' % key
    if edit:
        keys = sorted(to_list(x.keys()))
        y = other[-1]
        for key in keys:
            if key in hidden_fields:
                continue
            if x[key] == y[key]:
                continue
            if key[0] == '_':
                continue
            nkey = get_nkey(key)
            if nkey in x and nkey in y:
                continue
            print("    %s: %s -> %s" % (key, x[key], y[key]))
    elif create and options.verbose and table != 'tag_listing':
        keys = sorted(to_list(x.keys()))
        # the table keys have already been represented in the base format string
        also_hidden = list(_table_keys[table])
        also_hidden.extend([get_nkey(k) for k in also_hidden])
        for key in keys:
            if key in hidden_fields or key in also_hidden:
                continue
            nkey = get_nkey(key)
            if nkey in x:
                continue
            if key[0] == '_':
                continue
            if x.get('blocked') and key != 'blocked':
                continue
            if key.endswith('.name'):
                dkey = key[:-5]
            else:
                dkey = key
            print("    %s: %s" % (dkey, x[key]))


_table_keys = {
    'user_perms': ['user_id', 'perm_id'],
    'user_groups': ['user_id', 'group_id'],
    'cg_users': ['user_id', 'cg_id'],
    'tag_inheritance': ['tag_id', 'parent_id'],
    'tag_config': ['tag_id'],
    'tag_extra': ['tag_id', 'key'],
    'build_target_config': ['build_target_id'],
    'external_repo_config': ['external_repo_id'],
    'host_config': ['host_id'],
    'host_channels': ['host_id', 'channel_id'],
    'tag_external_repos': ['tag_id', 'external_repo_id'],
    'tag_listing': ['build_id', 'tag_id'],
    'tag_packages': ['package_id', 'tag_id'],
    'tag_package_owners': ['package_id', 'tag_id'],
    'group_config': ['group_id', 'tag_id'],
    'group_req_listing': ['group_id', 'tag_id', 'req_id'],
    'group_package_listing': ['group_id', 'tag_id', 'package'],
}


