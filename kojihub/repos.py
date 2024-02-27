import datetime
import koji
from koji.context import context
from .db import (
    DeleteProcessor,
    InsertProcessor,
    QueryProcessor,
    UpdateProcessor,
    _dml,
)
from .kojihub import (
    make_task,
    get_tag,
    RootExports,
)

'''
Repo regen queue
================

Repo regen queue is now in the database. Simple ordering by priority sorts it.

Tag can get into queue by:
1) kojira detecting that repo is no longer valid (priority 100)
2) By user requesting it via build with fresh repo (priority 90)
3) New target creation should add it to the queue (priority 80)
4) buildroot inheritance change (priority 100)

Tag can leave the queue:
1) tag is deleted (ON CASCADE?)
2) newRepo is started

Tag can be prioritized by tags.extra['repo_regen_priority']

Storing:
 * tag
 * last computed score + time of computation
 * needed since
 * expired since
 * priority (static offset)
 * awaited
 * maven_support

 Build can:
 1) run as normal (use latest repo)
 2) --wait-repo
      - if expired_event >= requested
        - newRepo is running, wait
      - expired_event < requested
        if newRepo is running, queue new one
 

Random ideas:
 - request_repo_regen can consult policy based on tag/user and set weight/priority
 - add start requests to the queue (this repo should be regenerated in two hours
   earliest if nobody will request it earlier
 - tag option - regenerate always, daily, never if not requested

repo_regen_method = 
    has req_regen_method :: req_regen_method
    method runroot :: never
    target match rhel-9.0.0-build :: always
    target match rhel-6.8.z-build :: 1d
    target match eng-fedora-34-* && bool has_external_repos :: check
    all :: none

Actions: always, 12h, 3d, check, never/manual

repo_regen_priority =
    perm match repo && has req_priority :: req_priority # via manual newRepo call
    perm match kpatch ::  adjust +1
    all :: heuristics
'''


def request_repo_regen(tag_id, priority=None, awaited=False):
    query = QueryProcessor(tables=['repo_queue'],
                           columns=['tag_id', 'priority', 'awaited'],
                           clauses=['tag_id = %(tag_id)s'],
                           values={'tag_id': tag_id},
                           opts={'rowlock': True})
    queued = query.executeOne()
    if queued:
        data = {}
        if priority and priority < queued['priority']:
            # better priority now
            data['priority'] = priority
        if awaited and not queued['awaited']:
            data['awaited'] = True

        if data:
            update = UpdateProcessor(table='repo_queue',
                                     clauses=['tag_id = %(tag_id)s'],
                                     values=queued)
            data['updated_ts'] = datetime.datetime.now()
            update.set(**data)
            update.execute()
    else:
        tag = get_tag(tag_id, strict=True)
        data = {
            'tag_id': tag_id,
            'maven_support': tag['maven_support'],
        }
        if awaited:
            data['awaited'] = awaited
        # TODO: improve getRepo import (put it out of RootExports)
        repo = RootExports().getRepo(tag_id)
        if repo:
            data['expired_event'] = repo['create_event']
            data['expired_ts'] = datetime.datetime.fromtimestamp(repo['create_ts'])
        if priority:
            context.session.assertPerm('regen-repo')
            data['priority'] = priority
        insert = InsertProcessor(table='repo_queue', data=data)
        insert.execute()


def set_repo_regen_priority(tag_id, priority):
    context.session.assertPerm('regen-repo')
    query = QueryProcessor(
        tables=['repo_regen'],
        columns=['priority'],
        clauses=['tag_id = %(tag_id)s'],
        values={'tag_id': tag_id},
    )
    queued = query.executeOne()
    if queued and queued['priority'] != priority:
        update = UpdateProcessor(
            'repo_queue',
            clauses=['tag_id = %(tag_id)s'],
            values={'tag_id': tag_id},
            data={'priority': priority},
        )
        update.execute()

    
def _get_top_repo(maven=None):
    # pop from the queue
    clauses = []
    values = {}
    if maven is not None:
        clauses.append('maven_support = %(maven)s')
        values = {'maven': bool(maven)}
    query = QueryProcessor(tables=['repo_queue'],
                           columns=['tag_id', 'priority', 'score'],
                           clauses=clauses,
                           values=values,
                           opts={
                             'order': 'priority,-score',
                             'limit': 1,
                             'rowlock': True,
                           })
    row = query.executeOne()
    if not row:
        return None

    remove_from_queue(row['tag_id'])
    return row['tag_id']


def remove_from_queue(tag_id):
    """Delete from queue in case tag
     - is no longer expired (task was scheduled)
     - doesn't exist anymore or 
     - is no more a buildtag
    """
    context.session.assertPerm('regen-repo')
    delete = DeleteProcessor('repo_queue',
                             clauses=['tag_id = %(tag_id)s'],
                             values={'tag_id': tag_id})
    delete.execute()


def get_repo_queue():
    # TODO, placeholder (CLI/web)
    columns = [
        'tag_id',
        'score',
        'priority',
        'expired_event',
        'expired_ts',
        'updated_ts',
        'weight',
        'awaited',
        #'max_n',
        'maven_support',
    ]
    query = QueryProcessor(tables=['repo_queue'],
                           columns=columns,
                           opts={'order': 'priority,-score'})
    return query.execute()


def update_score():
    """Set score for whole queue

    We score the tags by two factors
        - age of current repo
        - last use in a buildroot

    Having an older repo or a higher use count gives the tag a higher
    priority for regen. The formula attempts to keep the last use factor
    from overpowering, so that tags with very old repos still get priority

    Updating could happen periodically every few minutes, triggered by kojira
    """

    query = '''
    WITH subquery AS (
        WITH
            queued AS (
                SELECT
                   tag_id,
                    CASE awaited
                        WHEN TRUE THEN 2.0
                        ELSE 1.0
                    END awaited,
                   EXTRACT(epoch FROM (NOW() - expired_ts)) AS age
                FROM repo_queue),
            n_recent_values AS (
                SELECT COUNT(*) AS n_recent, repo.tag_id AS tag_id
                FROM buildroot
                LEFT OUTER JOIN standard_buildroot ON standard_buildroot.buildroot_id = buildroot.id
                LEFT OUTER JOIN repo ON repo.id = standard_buildroot.repo_id
                LEFT OUTER JOIN events ON events.id = standard_buildroot.create_event
                WHERE
                    events.time > NOW() - '1 year'::interval AND
                    repo.tag_id IN (SELECT tag_id FROM repo_queue)
                GROUP BY repo.tag_id),
            max_n AS (
                SELECT
                    CASE MAX(max_n)
                        WHEN 0 THEN 1
                        ELSE MAX(max_n)
                    END max_n
                FROM repo_queue
            )
        SELECT
            queued.tag_id,
            CASE
                WHEN n_recent IS NULL THEN age * awaited
                ELSE age * awaited * (CAST(n_recent * 9.0 AS INTEGER) / max_n.max_n + 1)
            END score
        FROM queued
        LEFT JOIN n_recent_values ON n_recent_values.tag_id = queued.tag_id
        JOIN max_n ON TRUE
    )
    UPDATE repo_queue
    SET score = score_table.score
    FROM (SELECT * FROM subquery) AS score_table
    WHERE score_table.tag_id = repo_queue.tag_id
    '''
    # TODO - maybe better as stored procedure?
    return _dml(query, {})


'''
def __update_score_original(tag_id):
    """Set score for needed_tag entry

    We score the tags by two factors
        - age of current repo
        - last use in a buildroot

    Having an older repo or a higher use count gives the tag a higher
    priority for regen. The formula attempts to keep the last use factor
    from overpowering, so that tags with very old repos still get priority

    Updating could happen periodically every few minutes, triggered by kojira
    """
    # get queue item
    query = QueryProcessor(tables=['repo_queue'],
                           columns=['awaited', 'EXTRACT(epoch FROM (NOW() - expired_ts))'],
                           aliases=['awaited', 'age'],
                           clauses=['tag_id = %(tag_id)s'],
                           values={'tag_id': tag_id})
    queued = query.executeOne()
    if not queued:
        return None

    # get recent uses in last day
    query = QueryProcessor(
        tables=['buildroot'],
        joins=[
            'LEFT OUTER JOIN standard_buildroot '
            'ON standard_buildroot.buildroot_id = buildroot.id',
            'LEFT OUTER JOIN repo '
            'ON repo.id = standard_buildroot.repo_id',
            'LEFT OUTER JOIN events '
            'ON events.id = standard_buildroot.create_event'],
        clauses=[
            'repo.tag_id = %(tag_id)s',
            "events.time > NOW() - '1 day'::interval",
        ],
        values={'tag_id': tag_id},
        opts={'countOnly': True})
    n_recent = query.executeOne()

    # SELECT max(n_recent) FROM repo_queue
    query = QueryProcessor(tables=['repo_queue'],
                           columns=['MAX(max_n)'],
                           aliases=['max'])
    max_n = query.executeOne()['max'] or 1

    adj = n_recent * 9.0 // max_n + 1
    age = queued['age']
    # XXX - need to make sure our times aren't far off, otherwise this
    # scoring could have the opposite of the desired effect
    if age < 0:
        age = 0
    if queued['awaited']:
        score_adjust = 2.0
    else:
        score_adjust = 1.0
    score = age * adj * score_adjust
    import logging
    logging.error(f"SCORE: {score}: age {age} adj {adj} score_adjust {score_adjust}")
    # so a day old unused repo gets about the regen same score as a
    # 2.4-hour-old, very popular repo

    # update queue
    update = UpdateProcessor(
        table='repo_queue',
        clauses=['tag_id = %(tag_id)s'],
        values={'tag_id': tag_id},
        data={'score': score},
        rawdata={'updated_ts': 'NOW()'}
    )
    update.execute()
    return score
'''


def start_newrepo_from_queue(task_priority=15, maven=None):
    """
    TODO: config rate-limiting (probably better in kojira?)
    could be done by non/maven task limits as arguments
    """
    # TODO - some kojira-only permission instead
    context.session.assertPerm('admin')
    while True:
        tag_id = _get_top_repo()
        if not tag_id:
            return
        if not get_tag(tag_id, strict=False):
            # deleted tag
            continue
        args = koji.encode_args(tag_id)
        return make_task('newRepo', args, priority=task_priority, channel='createrepo')


class ReposExports():
    request = staticmethod(request_repo_regen)
    setPriority = staticmethod(set_repo_regen_priority)
    removeFromQueue = staticmethod(remove_from_queue)
    startNewRepoFromQueue = staticmethod(start_newrepo_from_queue)
    updateScore = staticmethod(update_score)
