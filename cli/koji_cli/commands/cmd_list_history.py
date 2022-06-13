from __future__ import absolute_import, division

import logging
import os
import sys
import time
import traceback
from optparse import SUPPRESS_HELP, OptionParser

from six.moves import zip

import koji

from koji_cli.lib import (
    TimeOption,
    ensure_connection,
    error,
    get_usage_str,
    list_task_output_all_volumes
)


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


def _printTaskInfo(session, task_id, topdir, level=0, recurse=True, verbose=True):
    """Recursive function to print information about a task
       and its children."""

    BUILDDIR = '/var/lib/mock'
    indent = " " * 2 * level

    info = session.getTaskInfo(task_id)

    if info is None:
        raise koji.GenericError("No such task: %d" % task_id)

    if info['host_id']:
        host_info = session.getHost(info['host_id'])
    else:
        host_info = None
    buildroot_infos = session.listBuildroots(taskID=task_id)
    build_info = session.listBuilds(taskID=task_id)

    files = list_task_output_all_volumes(session, task_id)
    logs = []
    output = []
    for filename in files:
        if filename.endswith('.log'):
            logs += [os.path.join(koji.pathinfo.work(volume=volume),
                                  koji.pathinfo.taskrelpath(task_id),
                                  filename) for volume in files[filename]]
        else:
            output += [os.path.join(koji.pathinfo.work(volume=volume),
                                    koji.pathinfo.taskrelpath(task_id),
                                    filename) for volume in files[filename]]

    owner = session.getUser(info['owner'])['name']

    print("%sTask: %d" % (indent, task_id))
    print("%sType: %s" % (indent, info['method']))
    if verbose:
        print("%sRequest Parameters:" % indent)
        for line in _parseTaskParams(session, info['method'], task_id, topdir):
            print("%s  %s" % (indent, line))
    print("%sOwner: %s" % (indent, owner))
    print("%sState: %s" % (indent, koji.TASK_STATES[info['state']].lower()))
    print("%sCreated: %s" % (indent, time.asctime(time.localtime(info['create_ts']))))
    if info.get('start_ts'):
        print("%sStarted: %s" % (indent, time.asctime(time.localtime(info['start_ts']))))
    if info.get('completion_ts'):
        print("%sFinished: %s" % (indent, time.asctime(time.localtime(info['completion_ts']))))
    if host_info:
        print("%sHost: %s" % (indent, host_info['name']))
    if build_info:
        print("%sBuild: %s (%d)" % (indent, build_info[0]['nvr'], build_info[0]['build_id']))
    if buildroot_infos:
        print("%sBuildroots:" % indent)
        for root in buildroot_infos:
            print("%s  %s/%s-%d-%d/" %
                  (indent, BUILDDIR, root['tag_name'], root['id'], root['repo_id']))
    if logs:
        print("%sLog Files:" % indent)
        for log_path in logs:
            print("%s  %s" % (indent, log_path))
    if output:
        print("%sOutput:" % indent)
        for file_path in output:
            print("%s  %s" % (indent, file_path))

    # white space
    print('')

    if recurse:
        level += 1
        children = session.getTaskChildren(task_id, request=True)
        children.sort(key=lambda x: x['id'])
        for child in children:
            _printTaskInfo(session, child['id'], topdir, level, verbose=verbose)


