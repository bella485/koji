from __future__ import absolute_import, division

import logging
import pprint
import sys
import time
import traceback
from datetime import datetime
from dateutil.tz import tzutc
from optparse import SUPPRESS_HELP, OptionParser

from six.moves import zip

import koji

from koji.util import to_list
from koji_cli.lib import (
    TimeOption,
    ensure_connection,
    error,
    get_usage_str,
    warn,
)


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


def anon_handle_list_history(goptions, session, args):
    "[info] Display historical data"
    usage = "usage: %prog list-history [options]"
    parser = OptionParser(usage=get_usage_str(usage), option_class=TimeOption)
    # Don't use local debug option, this one stays here for backward compatibility
    # https://pagure.io/koji/issue/2084
    parser.add_option("--debug", action="store_true", default=goptions.debug, help=SUPPRESS_HELP)
    parser.add_option("--build", help="Only show data for a specific build")
    parser.add_option("--package", help="Only show data for a specific package")
    parser.add_option("--tag", help="Only show data for a specific tag")
    parser.add_option("--editor", "--by", metavar="USER",
                      help="Only show entries modified by user")
    parser.add_option("--user", help="Only show entries affecting a user")
    parser.add_option("--permission", help="Only show entries relating to a given permission")
    parser.add_option("--cg", help="Only show entries relating to a given content generator")
    parser.add_option("--external-repo", "--erepo",
                      help="Only show entries relating to a given external repo")
    parser.add_option("--build-target", "--target",
                      help="Only show entries relating to a given build target")
    parser.add_option("--group", help="Only show entries relating to a given group")
    parser.add_option("--host", help="Only show entries related to given host")
    parser.add_option("--channel", help="Only show entries related to given channel")
    parser.add_option("--xkey", help="Only show entries related to given tag extra key")
    parser.add_option("--before", type="time",
                      help="Only show entries before this time, " + TimeOption.get_help())
    parser.add_option("--after", type="time",
                      help="Only show entries after timestamp (same format as for --before)")
    parser.add_option("--before-event", metavar="EVENT_ID", type='int',
                      help="Only show entries before event")
    parser.add_option("--after-event", metavar="EVENT_ID", type='int',
                      help="Only show entries after event")
    parser.add_option("--watch", action="store_true", help="Monitor history data")
    parser.add_option("--active", action='store_true',
                      help="Only show entries that are currently active")
    parser.add_option("--revoked", action='store_false', dest='active',
                      help="Only show entries that are currently revoked")
    parser.add_option("--context", action="store_true", help="Show related entries")
    parser.add_option("-s", "--show", action="append", help="Show data from selected tables")
    parser.add_option("-v", "--verbose", action="store_true", help="Show more detail")
    parser.add_option("-e", "--events", action="store_true", help="Show event ids")
    parser.add_option("--all", action="store_true",
                      help="Allows listing the entire global history")
    parser.add_option("--utc", action="store_true",
                      help="Shows time in UTC timezone")
    (options, args) = parser.parse_args(args)
    if len(args) != 0:
        parser.error("This command takes no arguments")
    kwargs = {}
    limited = False
    for opt in ('package', 'tag', 'build', 'editor', 'user', 'permission',
                'cg', 'external_repo', 'build_target', 'group', 'before',
                'after', 'host', 'channel', 'xkey'):
        val = getattr(options, opt)
        if val:
            kwargs[opt] = val
            limited = True
    if options.before_event:
        kwargs['beforeEvent'] = options.before_event
    if options.after_event:
        kwargs['afterEvent'] = options.after_event
    if options.active is not None:
        kwargs['active'] = options.active
    if options.host:
        hostinfo = session.getHost(options.host)
        if not hostinfo:
            error("No such host: %s" % options.host)
    if options.channel:
        channelinfo = session.getChannel(options.channel)
        if not channelinfo:
            error("No such channel: %s" % options.channel)
    if options.utc:
        kwargs['utc'] = options.utc
    tables = None
    if options.show:
        tables = []
        for arg in options.show:
            tables.extend(arg.split(','))
    if not limited and not options.all:
        parser.error("Please specify an option to limit the query")

    ensure_connection(session, goptions)

    if options.watch:
        if not kwargs.get('afterEvent') and not kwargs.get('after'):
            kwargs['afterEvent'] = session.getLastEvent()['id']

    while True:
        histdata = session.queryHistory(tables=tables, **kwargs)
        timeline = []

        def distinguish_match(x, name):
            """determine if create or revoke event matched"""
            if options.context:
                return True
            name = "_" + name
            ret = True
            for key in x:
                if key.startswith(name):
                    ret = ret and x[key]
            return ret
        for table in histdata:
            hist = histdata[table]
            for x in hist:
                if x['revoke_event'] is not None:
                    if distinguish_match(x, 'revoked'):
                        timeline.append((x['revoke_event'], table, 0, x.copy()))
                    # pprint.pprint(timeline[-1])
                if distinguish_match(x, 'created'):
                    timeline.append((x['create_event'], table, 1, x))
        timeline.sort(key=lambda entry: entry[:3])
        # group edits together
        new_timeline = []
        last_event = None
        edit_index = {}
        for entry in timeline:
            event_id, table, create, x = entry
            if event_id != last_event:
                edit_index = {}
                last_event = event_id
            key = tuple([x[k] for k in _table_keys[table]])
            prev = edit_index.get((table, event_id), {}).get(key)
            if prev:
                prev[-1].setdefault('.related', []).append(entry)
            else:
                edit_index.setdefault((table, event_id), {})[key] = entry
                new_timeline.append(entry)
        for entry in new_timeline:
            if options.debug:
                print("%r" % list(entry))
            _print_histline(entry, options=options)
        if not options.watch:
            break
        else:
            time.sleep(goptions.poll_interval)
            # repeat query for later events
            if last_event:
                kwargs['afterEvent'] = last_event


def _handleMap(lines, data, prefix=''):
    for key, val in data.items():
        if key != '__starstar':
            lines.append('  %s%s: %s' % (prefix, key, val))


def _handleOpts(lines, opts, prefix=''):
    if opts:
        lines.append("%sOptions:" % prefix)
        _handleMap(lines, opts, prefix)


def _parseTaskParams(session, method, task_id, topdir):
    try:
        return _do_parseTaskParams(session, method, task_id, topdir)
    except Exception:
        logger = logging.getLogger("koji")
        if logger.isEnabledFor(logging.DEBUG):
            tb_str = ''.join(traceback.format_exception(*sys.exc_info()))
            logger.debug(tb_str)
        return ['Unable to parse task parameters']


def _do_parseTaskParams(session, method, task_id, topdir):
    """Parse the return of getTaskRequest()"""
    params = session.getTaskRequest(task_id)

    lines = []

    if method == 'buildSRPMFromCVS':
        lines.append("CVS URL: %s" % params[0])
    elif method == 'buildSRPMFromSCM':
        lines.append("SCM URL: %s" % params[0])
    elif method == 'buildArch':
        lines.append("SRPM: %s/work/%s" % (topdir, params[0]))
        lines.append("Build Tag: %s" % session.getTag(params[1])['name'])
        lines.append("Build Arch: %s" % params[2])
        lines.append("SRPM Kept: %r" % params[3])
        if len(params) > 4:
            _handleOpts(lines, params[4])
    elif method == 'tagBuild':
        build = session.getBuild(params[1])
        lines.append("Destination Tag: %s" % session.getTag(params[0])['name'])
        lines.append("Build: %s" % koji.buildLabel(build))
    elif method == 'buildNotification':
        build = params[1]
        buildTarget = params[2]
        lines.append("Recipients: %s" % (", ".join(params[0])))
        lines.append("Build: %s" % koji.buildLabel(build))
        lines.append("Build Target: %s" % buildTarget['name'])
        lines.append("Web URL: %s" % params[3])
    elif method == 'build':
        lines.append("Source: %s" % params[0])
        lines.append("Build Target: %s" % params[1])
        if len(params) > 2:
            _handleOpts(lines, params[2])
    elif method == 'maven':
        lines.append("SCM URL: %s" % params[0])
        lines.append("Build Target: %s" % params[1])
        if len(params) > 2:
            _handleOpts(lines, params[2])
    elif method == 'buildMaven':
        lines.append("SCM URL: %s" % params[0])
        lines.append("Build Tag: %s" % params[1]['name'])
        if len(params) > 2:
            _handleOpts(lines, params[2])
    elif method == 'wrapperRPM':
        lines.append("Spec File URL: %s" % params[0])
        lines.append("Build Tag: %s" % params[1]['name'])
        if params[2]:
            lines.append("Build: %s" % koji.buildLabel(params[2]))
        if params[3]:
            lines.append("Task: %s %s" % (params[3]['id'], koji.taskLabel(params[3])))
        if len(params) > 4:
            _handleOpts(lines, params[4])
    elif method == 'chainmaven':
        lines.append("Builds:")
        for package, opts in params[0].items():
            lines.append("  " + package)
            _handleMap(lines, opts, prefix="  ")
        lines.append("Build Target: %s" % params[1])
        if len(params) > 2:
            _handleOpts(lines, params[2])
    elif method == 'winbuild':
        lines.append("VM: %s" % params[0])
        lines.append("SCM URL: %s" % params[1])
        lines.append("Build Target: %s" % params[2])
        if len(params) > 3:
            _handleOpts(lines, params[3])
    elif method == 'vmExec':
        lines.append("VM: %s" % params[0])
        lines.append("Exec Params:")
        for info in params[1]:
            if isinstance(info, dict):
                _handleMap(lines, info, prefix='  ')
            else:
                lines.append("  %s" % info)
        if len(params) > 2:
            _handleOpts(lines, params[2])
    elif method in ('createLiveCD', 'createAppliance', 'createLiveMedia'):
        argnames = ['Name', 'Version', 'Release', 'Arch', 'Target Info', 'Build Tag', 'Repo',
                    'Kickstart File']
        for n, v in zip(argnames, params):
            lines.append("%s: %s" % (n, v))
        if len(params) > 8:
            _handleOpts(lines, params[8])
    elif method in ('appliance', 'livecd', 'livemedia'):
        argnames = ['Name', 'Version', 'Arches', 'Target', 'Kickstart']
        for n, v in zip(argnames, params):
            lines.append("%s: %s" % (n, v))
        if len(params) > 5:
            _handleOpts(lines, params[5])
    elif method == 'newRepo':
        tag = session.getTag(params[0])
        lines.append("Tag: %s" % tag['name'])
    elif method == 'prepRepo':
        lines.append("Tag: %s" % params[0]['name'])
    elif method == 'createrepo':
        lines.append("Repo ID: %i" % params[0])
        lines.append("Arch: %s" % params[1])
        oldrepo = params[2]
        if oldrepo:
            lines.append("Old Repo ID: %i" % oldrepo['id'])
            lines.append("Old Repo Creation: %s" % koji.formatTimeLong(oldrepo['create_ts']))
        if len(params) > 3:
            lines.append("External Repos: %s" %
                         ', '.join([ext['external_repo_name'] for ext in params[3]]))
    elif method == 'tagNotification':
        destTag = session.getTag(params[2])
        srcTag = None
        if params[3]:
            srcTag = session.getTag(params[3])
        build = session.getBuild(params[4])
        user = session.getUser(params[5])

        lines.append("Recipients: %s" % ", ".join(params[0]))
        lines.append("Successful?: %s" % (params[1] and 'yes' or 'no'))
        lines.append("Tagged Into: %s" % destTag['name'])
        if srcTag:
            lines.append("Moved From: %s" % srcTag['name'])
        lines.append("Build: %s" % koji.buildLabel(build))
        lines.append("Tagged By: %s" % user['name'])
        lines.append("Ignore Success?: %s" % (params[6] and 'yes' or 'no'))
        if params[7]:
            lines.append("Failure Message: %s" % params[7])
    elif method == 'dependantTask':
        lines.append("Dependant Tasks: %s" % ", ".join([str(depID) for depID in params[0]]))
        lines.append("Subtasks:")
        for subtask in params[1]:
            lines.append("  Method: %s" % subtask[0])
            lines.append("  Parameters: %s" %
                         ", ".join([str(subparam) for subparam in subtask[1]]))
            if len(subtask) > 2 and subtask[2]:
                subopts = subtask[2]
                _handleOpts(lines, subopts, prefix='  ')
            lines.append("")
    elif method == 'chainbuild':
        lines.append("Build Groups:")
        group_num = 0
        for group_list in params[0]:
            group_num += 1
            lines.append("  %i: %s" % (group_num, ', '.join(group_list)))
        lines.append("Build Target: %s" % params[1])
        if len(params) > 2:
            _handleOpts(lines, params[2])
    elif method == 'waitrepo':
        lines.append("Build Target: %s" % params[0])
        if params[1]:
            lines.append("Newer Than: %s" % params[1])
        if params[2]:
            lines.append("NVRs: %s" % ', '.join(params[2]))

    return lines
