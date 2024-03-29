import json
import logging
import os
import os.path
import time

import koji
import kojihub

from koji.context import context
from kojihub.db import (QueryView, UpdateProcessor, BulkUpdateProcessor, InsertProcessor, nextval,
                        Savepoint, QueryProcessor, db_lock, DeleteProcessor)


logger = logging.getLogger('koji.repo')


class RepoQuery(QueryView):

    tables = ['repo']
    joinmap = {
        'tag': 'tag ON repo.tag_id = tag.id',
        'create_ev': 'events AS create_ev ON repo.create_event = create_ev.id',
        'begin_ev': 'LEFT JOIN events AS begin_ev ON repo.begin_event = begin_ev.id',
        'end_ev': 'LEFT JOIN events AS end_ev ON repo.end_event = end_ev.id',
    }
    fieldmap = {
        'id': ['repo.id', None],
        'tag_id': ['repo.tag_id', None],
        'creation_time': ['repo.creation_time', None],
        'creation_ts': ["date_part('epoch', repo.creation_time)", None],
        'state_time': ['repo.creation_time', None],
        'state_ts': ["date_part('epoch', repo.creation_time)", None],
        'create_event': ['repo.create_event', None],
        'create_ts': ["date_part('epoch', create_ev.time)", 'create_ev'],
        'begin_event': ['repo.begin_event', None],
        'begin_ts': ["date_part('epoch', begin_ev.time)", 'begin_ev'],
        'end_event': ['repo.end_event', None],
        'end_ts': ["date_part('epoch', end_ev.time)", 'end_ev'],
        'state': ['repo.state', None],
        'dist': ['repo.dist', None],
        'opts': ['repo.opts', None],
        'custom_opts': ['repo.custom_opts', None],
        'task_id': ['repo.task_id', None],
        'tag_name': ['tag.name', 'tag'],
    }
    default_fields = ('id', 'tag_id', 'create_event', 'state', 'dist', 'task_id', 'opts',
                      'custom_opts')
    # Note that we avoid joins by default


class RepoQueueQuery(QueryView):

    tables = ['repo_queue']
    joinmap = {
        'tag': 'tag ON repo_queue.tag_id = tag.id',
        'task': 'LEFT JOIN task ON repo_queue.task_id = task.id',
    }
    fieldmap = {
        'id': ['repo_queue.id', None],
        'create_time': ['repo_queue.create_time', None],
        'create_ts': ["date_part('epoch', repo_queue.create_time)", None],
        'tag_id': ['repo_queue.tag_id', None],
        'tag_name': ['tag.name', 'tag'],
        'at_event': ['repo_queue.at_event', None],
        'min_event': ['repo_queue.min_event', None],
        'opts': ['repo_queue.opts', None],
        'update_time': ['repo_queue.update_time', None],
        'update_ts': ["date_part('epoch', repo_queue.update_time)", None],
        'active': ['repo_queue.active', None],
        'task_id': ['repo_queue.task_id', None],
        'task_state': ['task.state', 'task'],
        'tries': ['repo_queue.tries', None],
        'repo_id': ['repo_queue.repo_id', None],
        'score': ['repo_queue.score', None],
    }
    default_fields = ('id', 'tag_id', 'at_event', 'min_event', 'score', 'create_ts',
                      'task_id', 'tries', 'repo_id', 'opts', 'active', 'update_ts')


def check_repo_queue():
    # called from scheduler and/or  kojira
    if not db_lock('repo-queue', wait=False):
        return

    clauses = [['repo_id', 'IS', None], ['active', 'IS', True]]
    fields = ('*', 'task_state')
    waiting = RepoQueueQuery(clauses, fields=fields, opts={'order': 'id'}).execute()
    logger.debug('Got %i waiting repo requests', len(waiting))
    # TODO better ordering

    n_tasks = 0
    max_tasks = context.opts['MaxRepoTasks']
    # TODO also track maven like kojira does
    q_updates = {}

    # first pass -- check on tasks
    tag_tasks = {}
    for req in waiting:
        updates = q_updates.setdefault(req['id'], {})

        # check on task, if any
        if not req['task_id']:
            continue

        logger.debug('Req with task: %r', req)
        retry = False
        if req['task_state'] == koji.TASK_STATES['CLOSED']:
            # finished, did we get a repo?
            repo = RepoQuery([['task_id', '=', req['task_id']]]).executeOne()
            if not repo:
                logger.error('Repo task did not produce repo: %i', req['task_state'])
                retry = True
            else:
                if valid_repo(req, repo):
                    logger.info('Got valid repo for request: %r', req)
                    # record repo_id and mark inactive
                    updates['repo_id'] = repo['id']
                    updates['active'] = False
                else:
                    # (valid_repo already logged an error)
                    retry = True
        elif req['task_state'] in (koji.TASK_STATES['CANCELED'], koji.TASK_STATES['FAILED']):
            logger.warning('Repo request task did not complete: %r', req)
            retry = True
        else:
            # task still active
            n_tasks += 1
            tag_tasks.setdefault(req['tag_id'], []).append([req['task_id'], req])

        if retry:
            # something went wrong with the task. retry if we can
            if req['tries'] > context.opts['RepoRetries']:
                logger.error('Retries exhausted for repo request: %r', req)
                updates['active'] = False
            else:
                # forget task id so it can be rescheduled
                updates['task_id'] = None
                req['task_id'] = None
                # tries is incremented later when we make the task

    logger.debug('Found %i active repo request tasks', n_tasks)

    # second pass -- trigger new tasks if we can
    for req in waiting:
        updates = q_updates.setdefault(req['id'], {})

        if n_tasks > max_tasks:
            logger.debug('Reached max_tasks=%i', max_tasks)
            continue

        if req['task_id']:
            continue

        active = tag_tasks.get(req['tag_id'])
        if active:
            # XXX this is a bit too restrictive
            # TODO factor in options/parameters/time
            logger.debug('Found %i active tasks for tag %s', len(active), req['tag_id'])
            continue

        # for now, we'll just do first come, first served
        logger.debug('Request needs task: %r', req)
        task_id = repo_queue_task(req)
        if not task_id:
            continue
        tag_tasks.setdefault(req['tag_id'], []).append([task_id, req])

        tries = req['tries'] or 0
        updates['task_id'] = task_id
        updates['tries'] = tries + 1
        logger.info('Created task %i for repo request %r', task_id, req)

    # third pass -- apply updates
    # TODO bulk update?
    made_updates = False
    for req in waiting:
        updates = q_updates.get(req['id'])
        if not updates:
            continue
        made_updates = True
        upd = UpdateProcessor('repo_queue', data=updates, clauses=['id = %(id)s'], values=req)
        upd.rawset(update_time='NOW()')
        upd.execute()

    # clean up
    if made_updates:
        # TODO we don't really need to trigger this so often
        clean_repo_queue()


def clean_repo_queue():
    """Delete old inactive entries from the repo queue"""
    # these entries need to persist for at least a little while after fulfillment so that
    # clients can find the results of their requests
    delete = DeleteProcessor(
        'repo_queue',
        clauses=['active IS FALSE', 'update_time < NOW() - %(age)s::interval'],
        values={'age': '%s minutes' % context.opts['RequestCleanTime']},
    )
    n = delete.execute()
    if n:
        logger.info('Cleaned %s repo queue entries', n)


def valid_repo(req, repo):
    if repo['tag_id'] != req['tag_id']:
        logger.error('Request %i got repo %i with wrong tag: got %s, expected %s',
                     req['id'], repo['id'], repo['tag_id'], req['tag_id'])
        return False
    if repo['state'] != koji.REPO_STATES['READY']:
        logger.error('Request %i got repo %i with wrong state: got %s',
                     req['id'], repo['id'], repo['state'])
        return False
    if req['at_event'] is not None:
        if repo['create_event'] != req['at_event']:
            logger.error('Request %i got repo %i at wrong event: %s != %s',
                         req['id'], repo['id'], repo['create_event'], req['at_event'])
            return False
    elif repo['create_event'] < req['min_event']:
        logger.error('Request %i got repo %i before min_event: %s < %s',
                     req['id'], repo['id'], repo['create_event'], req['min_event'])
        return False
    if req['opts'] is None:
        if repo['custom_opts']:
            logger.error('Requested repo has unexpected custom opts: %r %r', req, repo)
    else:
        # all request options should have applied
        for key in req['opts']:
            if req['opts'][key] != repo.get('opts', {})[key]:
                logger.error('Requested repo has wrong opts: %r %r', req, repo)
                return False
        # any custom options should come from request
        for key in repo.get('custom_opts', {}):
            if repo['custom_opts'][key] != req['opts'][key]:
                logger.error('Requested repo has wrong opts: %r %r', req, repo)
                return False

    return True


def repo_done_hook(repo_id):
    """Check if newly ready repo satisfies requests"""
    savepoint = Savepoint('repo_done_hook')
    try:
        repo = RepoQuery([['id', '=', repo_id]]).executeOne()
        if not repo:
            # shouldn't happen, but...
            logger.error('No such repo: %i', repo_id)
            return
        if repo['dist']:
            return
        opts = repo['opts']
        custom = repo['custom_opts']
        if opts is None or custom is None:
            # XXX should not happen
            logger.error('Repo with invalid opts values: %r', repo)
            return

        # query for matching requests
        fields = ['id']
        qopts = {'order': 'id'}
        base_clauses = [
            ['tag_id', '=', repo['tag_id']],
            ['repo_id', 'IS', None],
            ['opts', '<@', json.dumps(opts)],
            ['opts', '@>', json.dumps(custom)],
            # i.e. repo matches all opts in request and request matches all custom opts in repo
        ]
        # TODO adjust this once QueryView supports OR
        clauses = base_clauses + [['min_event', '<=', repo['create_event']]]
        reqs1 = RepoQueueQuery(clauses, fields, qopts).execute()
        clauses = base_clauses + [['at_event', '=', repo['create_event']]]
        reqs2 = RepoQueueQuery(clauses, fields, qopts).execute()
        reqs = reqs1 + reqs2

        # and update!
        if reqs:
            update = UpdateProcessor('repo_queue',
                                     clauses=['id IN %(ids)s'],
                                     values={'ids': [r['id'] for r in reqs]},
                                     data={'repo_id': repo['id']})
            # TODO should we also update task_id?
            update.execute()
    except Exception:
        # We're being very careful since we're a callback
        savepoint.rollback()
        logger.exception('Failed to update repo queue')


def symlink_if_latest(repo):
    """Point latest symlink at repo, if appropriate

    :param dict repo: repo data
    :returns: bool

    Returns True if the latest symlink was updated, False otherwise
    """

    if not repo['dist'] and not repo['opts'].get('custom'):
        # only symlink if we have the default opts
        logger.debug('Skipping latest symlink. Not default opts.')
        return False

    # only symlink if we are actually latest
    clauses = [
        ['tag_id', '=', repo['tag_id']],
        ['state', '=', koji.REPO_READY],
        ['create_event', '>=', repo['create_event']]]
    if repo['dist']:
        clauses.append(['dist', 'IS', True])
    else:
        clauses.append(['custom_opts', '=', '{}'])
    query = RepoQuery(clauses)
    newer = query.execute()
    if newer:
        logger.debug('Skipping latest symlink, %i newer repos found', len(newer))
        return False

    if repo['dist']:
        latestrepolink = koji.pathinfo.distrepo('latest', repo['tag_name'])
    else:
        latestrepolink = koji.pathinfo.repo('latest', repo['tag_name'])
        # TODO - avoid abusing pathinfo like this
    try:
        if os.path.lexists(latestrepolink):
            os.unlink(latestrepolink)
        os.symlink(str(repo['id']), latestrepolink)
    except OSError:
        # making this link is nonessential
        logger.error("Unable to create latest link: %s" % latestrepolink)
        return False
    return True


def repo_queue_task(req):
    opts = req['opts'] or {}
    # should already be valid, but just in case
    opts = convert_repo_opts(opts, strict=True)
    kwargs = {'opts': opts}
    if req['at_event'] is not None:
        kwargs['event'] = req['at_event']
    # otherwise any new repo will satisfy any valid min_event

    args = koji.encode_args(req['tag_id'], **kwargs)
    taskopts = {'priority': 15, 'channel': 'createrepo'}
    user_id = kojihub.get_id('users', context.opts['RepoQueueUser'], strict=False)
    # TODO should we error if user doesn't exist
    if user_id:
        taskopts['owner'] = user_id
    task_id = kojihub.make_task('newRepo', args, **taskopts)
    return task_id
    # caller should update request entry if needed


def update_end_events():
    """Update end_event for all ready repos that don't have one yet"""
    query = RepoQuery(
        clauses=[['end_event', 'IS', None], ['state', '=', koji.REPO_READY]],
        fields=('id', 'tag_id', 'create_event'),
        opts={'order': 'id'})
    repos = query.execute()
    n_cached = 0
    tag_last = {}
    updates = []
    for repo in query.execute():
        # TODO is it worth using iterate() here?
        tag_id = repo['tag_id']
        # use cache to avoid redundant calls
        if tag_id in tag_last and tag_last[tag_id] <= repo['create_event']:
            # we already know that tag hasn't changed
            n_cached += 1
            continue
        end_event = kojihub.tag_first_change_event(repo['tag_id'], after=repo['create_event'])
        if end_event is None:
            tag_last[tag_id] = kojihub.tag_last_change_event(tag_id)
        else:
            updates.append({'id': repo['id'], 'end_event': end_event})
    if updates:
        BulkUpdateProcessor('repo', data=updates, match_keys=('id',)).execute()
    logger.debug('Checked end events for %i repos', len(repos))
    logger.debug('Got no change for %i distinct tags', len(tag_last))
    logger.debug('Avoided %i checks due to cache', n_cached)
    logger.debug('Added end events for %i repos', len(updates))


def get_external_repo_data(erepo):
    external_repo_id = kojihub.get_external_repo_id(erepo, strict=True)
    query = QueryProcessor(
        tables=['external_repo_data'],
        clauses=['external_repo_id = %(id)s', 'active IS TRUE'],
        columns=['data'],
        values={'id': external_repo_id})
    return query.singleValue(strict=False)


def set_external_repo_data(erepo, data):
    """Update tracking data for an external repo

    This is intended to be used by kojira
    """

    external_repo_id = kojihub.get_external_repo_id(erepo, strict=True)
    data = kojihub.convert_value(data, cast=dict)

    values = {
        'external_repo_id': external_repo_id,
        'data': json.dumps(data),
    }

    # revoke old entry, if any
    update = UpdateProcessor(
        table='external_repo_data',
        clauses=['external_repo_id = %(external_repo_id)s'],
        values=values)
    update.make_revoke()
    update.execute()

    # insert new entry
    insert = InsertProcessor(table='external_repo_data', data=values)
    insert.make_create()
    insert.execute()


def do_auto_requests():
    """Request repos for tag configured to auto-regen"""

    # query the extra configs we need
    query = QueryProcessor(
        tables=['tag_extra'],
        columns=['tag_id', 'key', 'value'],
        clauses=['key IN %(keys)s', 'active IS TRUE'],
        values={'keys': ['repo.auto', 'repo.lag']})

    # figure out which tags to handle and if they have lag settings
    auto_tags = []
    lags = {}
    for row in query.execute():
        if row is None:
            # blocked entry, ignore
            continue
        # tag_extra values are TEXT, but contain json values
        try:
            value = json.loads(row['value'])
        except Exception:
            # logging will be too noisy if it actually happens
            continue
        if row['key'] == 'repo.auto':
            if value:
                auto_tags.append(row['tag_id'])
        elif row['key'] == 'repo.lag':
            if not isinstance(value, int):
                # just ignore
                continue
            lags[row['tag_id']] = value

    logger.debug('Found %i tags for automatic repos', len(auto_tags))

    reqs = {}
    dups = {}
    default_lag = context.opts['RepoAutoLag']
    window = context.opts['RepoLagWindow']
    for tag_id in auto_tags:
        # choose min_event similar to default_min_event, but different lag
        # TODO unify code?
        last = kojihub.tag_last_change_event(tag_id)
        if last is None:
            # shouldn't happen
            # last event cannot be None for a valid tag, but we only queried tag_extra
            logger.error('No last event for tag %i', tag_id)
            continue
        lag = lags.get(tag_id, default_lag)
        base_ts = time.time() - lag
        base_ts = (base_ts // window) * window
        base = context.handlers.get('getLastEvent')(before=base_ts)['id']
        check = request_repo(tag_id, min_event=min(base, last))
        # TODO create a way to deprioritize these
        if check['duplicate']:
            dups[tag_id] = check
        elif check['request']:
            reqs[tag_id] = check

    logger.debug('Auto repo requests: %s', len(reqs))
    logger.debug('Auto repo duplicates: %s', len(dups))


def old_get_repo(tag, state=None, event=None, dist=False, min_event=None):
    """Get individual repository data based on tag and additional filters.
    If more repos fits, most recent is returned.

    :param int|str tag: tag ID or name
    :param int state: value from koji.REPO_STATES
    :param int event: maximum event ID. legacy arg
    :param bool dist: True = dist repo, False = regular repo
    :param int min_event: minimum event ID

    :returns: dict with repo data
    """
    tag_id = kojihub.get_tag_id(tag, strict=True)
    state = kojihub.convert_value(state, int, none_allowed=True)
    event = kojihub.convert_value(event, int, none_allowed=True)
    min_event = kojihub.convert_value(min_event, int, none_allowed=True)
    dist = kojihub.convert_value(dist, bool)

    fields = '**'
    clauses = [['tag_id', '=', tag_id]]
    if dist:
        clauses.append(['dist', 'IS', True])
    else:
        clauses.append(['dist', 'IS', False])
    if event:
        # the event arg was originally used to report a repo for an old tag event
        # hence, looking for events before that and omitting the state
        clauses.append(['create_event', '<=', event])
    else:
        if state is None:
            state = koji.REPO_READY
        clauses.append(['state', '=', state])
    if min_event is not None:
        clauses.append(['create_event', '>=', min_event])

    opts = {'order': '-creation_time', 'limit': 1}
    return RepoQuery(clauses, fields, opts).executeOne()


def get_repo(tag, min_event=None, at_event=None, opts=None):
    """Get best ready repo matching given requirements

    :param int|str tag: tag ID or name
    :param int min_event: minimum event ID
    :param int at_event: specific event ID
    :param dict opts: repo options

    :returns: dict with repo data
    """
    tag_id = kojihub.get_tag_id(tag, strict=True)
    min_event = kojihub.convert_value(min_event, int, none_allowed=True)
    at_event = kojihub.convert_value(at_event, int, none_allowed=True)
    opts = convert_repo_opts(opts, strict=True)

    fields = '**'
    clauses = [
        ['tag_id', '=', tag_id],
        ['dist', 'IS', False],
        ['state', '=', koji.REPO_READY],
        ['opts', '@>', json.dumps(opts)],
        ['custom_opts', '<@', json.dumps(opts)],
    ]
    # TODO: should we expand usage to include dist?
    if at_event is not None:
        clauses.append(['create_event', '=', at_event])
    elif min_event is not None:
        clauses.append(['create_event', '>=', min_event])

    qopts = {'order': '-create_event', 'limit': 1}
    return RepoQuery(clauses, fields, qopts).executeOne()


def get_repo_opts(tag, override=None):
    """Determine repo options from taginfo and apply given overrides

    :param dict tag: taginfo (via get_tag)
    :param dict|None override: repo options to override. optional.
    :returns: opts, custom

    Returns a pair of option dictionaries: opts, custom
    - opts gives the repo options with overrides applied
    - custom gives effective overrides (those that differed from tag default)
    """

    # base options
    opts = {
        'src': False,
        'debuginfo': False,
        'separate_src': False,
    }

    # emulate original kojira config
    debuginfo_pat = context.opts['DebuginfoTags'].split()
    src_pat = context.opts['SourceTags'].split()
    separate_src_pat = context.opts['SeparateSourceTags'].split()
    if debuginfo_pat:
        if koji.util.multi_fnmatch(tag['name'], debuginfo_pat):
            opts['debuginfo'] = True
    if src_pat:
        if koji.util.multi_fnmatch(tag['name'], src_pat):
            opts['src'] = True
    if separate_src_pat:
        if koji.util.multi_fnmatch(tag['name'], separate_src_pat):
            opts['separate_src'] = True

    # also consider tag config
    tag_opts = tag['extra'].get('repo.opts', {})
    if 'with_debuginfo' in tag['extra']:
        # for compat with old newRepo
        if 'debuginfo' in tag_opts:
            logger.warning('Ignoring legacy with_debuginfo config, overridden by repo.opts')
        else:
            tag_opts['debuginfo'] = bool(tag['extra']['with_debuginfo'])
    tag_opts = convert_repo_opts(tag_opts, strict=False)
    opts.update(tag_opts)

    # apply overrides
    custom = {}
    if override is not None:
        override = convert_repo_opts(override)
        custom = {k: override[k] for k in override if override[k] != opts[k]}
        opts.update(custom)

    return opts, custom


def convert_repo_opts(opts, strict=False):
    """Ensure repo_opts has correct form

    :param dict|None opts: repo options
    :param bool strict: error if opts are invalid
    :returns: (opts, full)

    Returns updated opts dictionary.
    If strict is true, will error on invalid opt values, otherwise they are ignored
    """

    if opts is None:
        return {}

    if not isinstance(opts, dict):
        if strict:
            raise koji.ParameterError('Repo opts must be a dictionary')
        else:
            logger.warning('Ignoring invalid repo opts: %r', opts)
            return {}

    all_opts = {'src', 'debuginfo', 'separate_src'}
    new_opts = {}
    for key in opts:
        if key not in all_opts:
            if strict:
                raise koji.ParameterError(f'Invalid repo option: {key}')
            else:
                logger.warning('Ignoring invalid repo opt: %s', key)
                continue
        # at the moment, all known opts are boolean, so this is fairly easy
        value = opts[key]
        if value is None:
            # treat as unspecified
            logger.info('Received None value in repo opts: %r', opts)
            continue
        new_opts[key] = kojihub.convert_value(value, bool)

    return new_opts


def request_repo(tag, min_event=None, at_event=None, opts=None, force=False):
    """Request a repo for a tag

    :param int|str taginfo: tag id or name
    :param int|str min_event: minimum event for the repo (optional)
    :param int at_event: specific event for the repo (optional)
    :param dict opts: custom repo options (optional)
    :param bool force: force request creation, even if a matching repo exists

    The special value min_event="last" uses the most recent event for the tag
    Otherwise min_event should be an integer

    use opts=None (the default) to get default options for the tag.
    If opts is given, it should be a dictionary of repo options. These will override
    the defaults.
    """

    context.session.assertLogin()
    taginfo = kojihub.get_tag(tag, strict=True)
    opts = convert_repo_opts(opts, strict=True)
    if at_event is not None:
        if min_event is not None:
            raise koji.ParameterError('The min_event and at_event options conflict')
        at_event = kojihub.convert_value(at_event, cast=int)
        ev = context.handlers.get('getEvent')(at_event, strict=False)
        if not ev:
            raise koji.ParameterError(f'Invalid event: {at_event}')
    elif min_event == "last":
        min_event = kojihub.tag_last_change_event(taginfo['id'])
        logger.debug('Using last event %s for repo request', min_event)
    elif min_event is None:
        min_event = default_min_event(taginfo)
        logger.debug('Using event %s for repo request', min_event)
    else:
        min_event = kojihub.convert_value(min_event, cast=int)
        ev = context.handlers.get('getEvent')(min_event, strict=False)
        if not ev:
            raise koji.ParameterError(f'Invalid event: {min_event}')

    ret = {'repo': None, 'request': None, 'duplicate': False}

    if not force:
        # do we have an existing repo?
        repo = get_repo(taginfo['id'], min_event=min_event, at_event=at_event, opts=opts)
        if repo:
            ret['repo'] = repo
            # TODO: do we need to record a request entry for stats?
            return ret

    # do we have a matching request already?
    clauses = [
        ['tag_id', '=', taginfo['id']],
        ['opts', '=', json.dumps(opts)],
    ]
    if at_event is not None:
        clauses.append(['at_event', '=', at_event])
    else:
        clauses.append(['min_event', '>=', min_event])
    check = RepoQueueQuery(clauses, fields='**', opts={'order': 'id'}).execute()
    for req in check:
        # if there is more than one, the oldest is most likely to be satisfied first
        # TODO update score/data/stats??
        ret['request'] = req
        ret['duplicate'] = True
        return ret

    # otherwise we make one
    req_id = nextval('repo_queue_id_seq')
    data = {
        'id': req_id,
        'tag_id': taginfo['id'],
        'at_event': at_event,
        'min_event': min_event,
        'opts': json.dumps(opts),
        # score?
    }
    insert = InsertProcessor('repo_queue', data=data)
    insert.execute()
    logger.info('New repo request for %(name)s', taginfo)

    # query to make return consistent with above
    req = RepoQueueQuery(clauses=[['id', '=', req_id]], fields='**').executeOne()
    ret['request'] = req
    return ret


def default_min_event(taginfo):
    """Get the default min_event for repo requests"""
    last = kojihub.tag_last_change_event(taginfo['id'])
    # last event cannot be None for a valid tag
    lag = taginfo['extra'].get('repo.lag')
    if lag is not None and not isinstance(lag, int):
        logger.warning('Invalid repo.lag setting for tag %s: %r', taginfo['name'], lag)
        lag = None
    if lag is None:
        lag = context.opts['RepoLag']
    window = context.opts['RepoLagWindow']
    base_ts = time.time() - lag
    # We round base_ts to nearest window so that duplicate requests will get same event if they
    # are close in time.
    base_ts = (base_ts // window) * window
    base = context.handlers.get('getLastEvent')(before=base_ts)['id']
    # If the tag has changed recently, we allow a bit of lag.
    # Otherwise, we use the most recent event for the tag.
    return min(base, last)


def check_repo_request(req_id):
    """Report status of repo request

    :param int req_id the request id
    :return: status dictionary

    The return dictionary will include 'request' and 'repo' fields
    """
    req_id = kojihub.convert_value(req_id, int)
    clauses = [['id', '=', req_id]]
    req = RepoQueueQuery(clauses, fields='**').executeOne()
    if not req:
        raise koji.GenericError('No such request')

    ret = {'repo': None, 'request': req}

    # do we have a repo yet?
    if req['repo_id']:
        ret['repo'] = kojihub.repo_info(req['repo_id'])

    # do we have a task?
    elif req['task_id']:
        ret['task'] = kojihub.Task(req['task_id']).getInfo()

    return ret


class RepoExports:

    request = staticmethod(request_repo)
    get = staticmethod(get_repo)
    checkRequest = staticmethod(check_repo_request)

    getExternalRepoData = staticmethod(get_external_repo_data)

    def query(self, clauses, fields=None, opts=None):
        query = RepoQuery(clauses, fields, opts)
        return query.iterate()

    def setExternalRepoData(self, external_repo_id, data):
        """Update tracking data for an external repo"""
        context.session.assertPerm('repo')
        set_external_repo_data(external_repo_id, data)

    def autoRequests(self):
        """[kojira] trigger automatic repo requests"""
        context.session.assertPerm('repo')
        # TODO this probably needs a new perm
        do_auto_requests()

    def checkQueue(self):
        """[kojira] trigger automatic repo requests"""
        context.session.assertPerm('repo')
        # TODO this probably needs a new perm
        check_repo_queue()

    def updateEndEvents(self):
        """[kojira] update end events for repos"""
        context.session.assertPerm('repo')
        # TODO this probably needs a new perm
        update_end_events()


# the end
