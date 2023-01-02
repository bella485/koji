import functools
import inspect
import logging

import koji
from koji.context import context
from koji.db import (
    BulkInsertProcessor,
    DeleteProcessor,
    InsertProcessor,
    QueryProcessor,
    UpdateProcessor,
)

logger = logging.getLogger('koji.scheduler')


class HostHashTable(object):
    """multiindexed host table for fast filtering"""
    def __init__(self, hosts=None):
        self.arches = {}
        self.channels = {}
        self.methods = {}
        self.hosts = {}
        self.host_ids = set()
        self.old_hosts = set()
        if hosts is None:
            hosts = get_ready_hosts()
        for hostinfo in hosts:
            self.add_host(hostinfo)

    def add_host(self, hostinfo):
        host_id = hostinfo['id']
        # priority is based on available capacity
        hostinfo['priority'] = hostinfo['capacity'] - hostinfo['task_load']
        # but builders running zero tasks should be always better fit
        if hostinfo['task_load'] == 0:
            # TODO: better heuristic?
            hostinfo['priority'] += 100
        # TODO: one query for all hosts
        # TODO: add only hosts which checked in in last X minutes
        query = QueryProcessor(tables=['task'], clauses=['host_id = %(host_id)i'],
                               values={'host_id': host_id}, opts={'countOnly': True})
        hostinfo['tasks'] = query.executeOne()

        self.hosts[host_id] = hostinfo
        self.host_ids.add(host_id)
        for arch in hostinfo['arches']:
            self.arches.setdefault(arch, set()).add(host_id)
        for channel in hostinfo['channels']:
            self.channels.setdefault(channel, set()).add(host_id)
        if not hostinfo['data']:
            self.old_hosts.add(host_id)
        else:
            for method in hostinfo['data']['methods']:
                self.methods.setdefault(method, set()).add(host_id)

        # know about refused tasks
        query = QueryProcessor(tables=['scheduler_task_runs'], columns=['task_id'],
                               clauses=['host_id = %(host_id)i', 'state = %(state)i'],
                               values={'host_id': host_id, 'state': koji.TASK_STATES['REFUSED']},
                               opts={'asList': True})
        self.hosts[host_id]['refused_tasks'] = set([x[0] for x in query.execute()])

    def get(self, task):
        # filter by requirements
        host_ids = set(self.host_ids)
        # assigned task
        if task['host_id']:
            host_ids &= {task['host_id']}
        # filter by architecture
        if task.get('arch') is not None:
            host_ids &= self.arches.get(task['arch'], set())
        # filter by method (unknown for old builders)
        host_ids &= self.methods.get(task['method'], set()) | self.old_hosts
        # filter by channel
        if task.get('channel_id') is not None:
            host_ids &= self.channels.get(task['channel_id'], set())

        # select best from filtered and remove hosts which already refused this task
        hosts = []
        # for old builder just heurstic of 1.5
        task_weight = 1.5
        for host_id in host_ids:
            hostinfo = self.hosts[host_id]
            if task['id'] in hostinfo['refused_tasks']:
                dblogger.debug("Task already refused", task_id=task['id'], host_id=host_id)
                continue
            if host_id not in self.old_hosts:
                task_weight = hostinfo['data']['methods'][task['method']]
            if task_weight > hostinfo['capacity'] - hostinfo['task_load']:
                dblogger.debug(
                    f"Higher weight {task_weight} than available capacity {hostinfo['capacity']}",
                    task_id=task['id'], host_id=host_id)
                continue
            if host_id not in self.old_hosts and hostinfo['data']['maxjobs'] < 1:
                dblogger.debug("Host has no free job slot", task_id=task['id'], host_id=host_id)
                continue
            hosts.append(hostinfo)

        hosts = sorted(hosts, key=lambda x: -x['priority'])
        if not hosts:
            return None

        host = hosts[0]
        # reduce resources (reserved memory, cpus)
        host['task_load'] += task_weight
        if host['id'] not in self.old_hosts:
            host['data']['maxjobs'] -= 1
        return host


def drop_from_queue(task_id):
    """Delete scheduled run without checking its existence"""
    delete = DeleteProcessor(
        table='scheduler_task_runs',
        clauses=['task_id = %(task_id)i'],
        values={'task_id': task_id},
    )
    delete.execute()


def get_host_data(hostID=None):
    """Return actual builder data

    :param int hostID: Return data for given host (otherwise for all)
    :returns list[dict]: list of host_id/data dicts
    """
    clauses = []
    columns = ['host_id', 'data']
    if hostID is not None:
        clauses.append('host_id = %(hostID)i')
    query = QueryProcessor(
        tables=['scheduler_host_data'],
        clauses=clauses,
        columns=columns,
        values=locals(),
        opts={'order': 'host_id'}
    )

    return query.execute()


def get_task_runs(taskID=None, hostID=None, states=None):
    """Return content of scheduler queue

    :param int taskID: filter by task
    :param int hostID: filter by host
    :param list[int] states: filter by states
    :returns list[dict]: list of dicts
    """

    fields = (
        ('id', 'id'),
        ('task_id', 'task_id'),
        ('host_id', 'host_id'),
        ('state', 'state'),
        ('create_time', 'create_time'),
        ("date_part('epoch', create_time)", 'create_ts'),
        ('start_time', 'start_time'),
        ("date_part('epoch', start_time)", 'start_ts'),
        ('end_time', 'end_time'),
        ("date_part('epoch', end_time)", 'end_ts'),
    )
    columns, aliases = zip(*fields)
    clauses = []
    if taskID is not None:
        clauses.append('task_id = %(taskID)i')
    if hostID is not None:
        clauses.append('host_id = %(hostID)i')
    if states is not None:
        clauses.append('state IN %(states)s')

    query = QueryProcessor(
        tables=['scheduler_task_runs'], columns=columns, aliases=aliases,
        clauses=clauses, values=locals(), opts={'order': 'id'},
    )
    return query.execute()


def get_ready_hosts():
    """Return information about hosts that are ready to build.

    Hosts set the ready flag themselves
    Note: We ignore hosts that are late checking in (even if a host
        is busy with tasks, it should be checking in quite often).

    host dict contains:
      - id
      - name
      - list(arches)
      - task_load
      - capacity
      - list(channels) (ids)
      - [resources]
    """
    query = QueryProcessor(
        tables=['host'],
        columns=['host.id', 'name', 'arches', 'task_load', 'capacity', 'data'],
        aliases=['id', 'name', 'arches', 'task_load', 'capacity', 'data'],
        clauses=[
            'enabled IS TRUE',
            'ready IS TRUE',
            'expired IS FALSE',
            'master IS NULL',
            'active IS TRUE',
            "update_time > NOW() - '5 minutes'::interval",
            'capacity > task_load',
        ],
        joins=[
            'sessions USING (user_id)',
            'host_config ON host.id = host_config.host_id',
            'LEFT JOIN scheduler_host_data ON host.id = scheduler_host_data.host_id',
        ]
    )
    hosts = query.execute()
    for host in hosts:
        query = QueryProcessor(
            tables=['host_channels'],
            columns=['channel_id'],
            clauses=['host_id=%(id)s', 'active IS TRUE', 'enabled IS TRUE'],
            joins=['channels ON host_channels.channel_id = channels.id'],
            values=host,
            opts={'asList': True},
        )
        rows = query.execute()
        host['channels'] = [row[0] for row in rows]
        host['arches'] = host['arches'].split() + ['noarch']
    return hosts


def clean_scheduler_queue():
    # FAIL inconsistent runs, but not tasks
    query = QueryProcessor(
        tables=['scheduler_task_runs', 'task'],
        columns=['scheduler_task_runs.id'],
        clauses=[
            'task.id = scheduler_task_runs.task_id',
            'scheduler_task_runs.state = %(state)s',
            'scheduler_task_runs.state != task.state',
        ],
        values={'state': koji.TASK_STATES['OPEN']},
        opts={'asList': True},
    )
    run_ids = [x[0] for x in query.execute()]
    # FAIL (timeout) also runs which are scheduled for too long and were not picked
    # by their respective workers, try to find new builders for them
    query = QueryProcessor(
        tables=['scheduler_task_runs'],
        columns=['id'],
        clauses=[
            "create_time < NOW() - '5 minutes'::interval",
            "state = %(state)i",
        ],
        values={'state': koji.TASK_STATES['SCHEDULED']},
        opts={'asList': True},
    )
    # TODO: does it make sense to have TIMEOUTED state for runs?
    run_ids += [x[0] for x in query.execute()]
    if run_ids:
        update = UpdateProcessor(
            table='scheduler_task_runs',
            clauses=['id IN %(run_ids)s'],
            values={'run_ids': run_ids},
            data={'state': koji.TASK_STATES['FAILED']},
            rawdata={'end_time': 'NOW()'},
        )
        update.execute()


def schedule(task_id=None):
    """Run scheduler"""

    # TODO: locking so, only one scheduler runs in a time
    # TODO: don't run it too often (configurable)
    # TODO: run only reasonably, now we trigger it on every updateHost + makeTask

    # stupid for now, just add new task to random builder
    logger.error("SCHEDULER RUN")
    hosts = HostHashTable()
    if not hosts.hosts:
        # early fail if there is nothing available
        dblogger.debug("Hosts not found")
        return

    # find unscheduled tasks
    columns = ['id', 'arch', 'method', 'channel_id', 'priority', 'host_id']
    if not task_id:
        clean_scheduler_queue()
        query = QueryProcessor(
            tables=['task'], columns=columns,
            clauses=[
                'state IN %(states)s',
                'id NOT IN (SELECT task_id FROM scheduler_task_runs WHERE state = 6)'
            ],
            values={'states': [koji.TASK_STATES['FREE'], koji.TASK_STATES['ASSIGNED']]},
            opts={'order': '-priority'},
        )
    else:
        query = QueryProcessor(
            tables=['task'], columns=columns,
            clauses=['id = %(id)i'], values={'id': task_id},
            opts={'order': '-priority'},
        )
    tasks = list(query.execute())

    # assign them to builders fulfiling criteria in priority order
    runs = []
    for task in tasks:
        host = hosts.get(task)
        if not host:
            # TODO: log that there is not available builder
            dblogger.warning("Can't find adequate builder", task_id=task['id'])
            continue
        runs.append({
            'host_id': host['id'],
            'task_id': task['id'],
            'state': koji.TASK_STATES['SCHEDULED'],
        })
        dblogger.info("Scheduling", task_id=task['id'], host_id=host['id'])
    insert = BulkInsertProcessor(table='scheduler_task_runs', data=runs)
    insert.execute()


class SchedulerExports():
    getTaskRuns = staticmethod(get_task_runs)
    getHostData = staticmethod(get_host_data)

    def getLogs(self, taskID=None, hostID=None, level=None,
                from_ts=None, to_ts=None, logger_name=None):
        """Return all related log messages

        :param int taskID: filter by task
        :param int hostID: filter by host
        :param str level: filter by message level
        :param float from_ts: filter from earliest time
        :param float to_ts: filter to latest time (from_ts < ts <= to_ts)
        :param str logger_name: filter by logger name
        :return [dict]: list of messages
        """
        fields = (
            ('scheduler_log_messages.id', 'id'),
            ('task_id', 'task_id'),
            ('host_id', 'host_id'),
            ('msg_time', 'msg_time'),
            ("date_part('epoch', msg_time)", 'msg_ts'),
            ('logger_name', 'logger_name'),
            ('level', 'level'),
            ('location', 'location'),
            ('msg', 'msg'),
            ('host.name', 'host_name'),
        )
        clauses = []
        values = {}
        if taskID is not None:
            clauses.append("task_id = %(taskID)i")
            values['taskID'] = taskID
        if hostID is not None:
            clauses.append("host_id = %(hostID)i")
            values['hostID'] = hostID
        if level is not None:
            clauses.append("level = %(level)s")
            values['level'] = level.upper()
        if from_ts is not None:
            clauses.append("msg_time > %(from_ts)s")
            values['from_ts'] = float(from_ts)
        if to_ts is not None:
            clauses.append("msg_time <= %(to_ts)s")
            values['to_ts'] = float(to_ts)
        if logger_name is not None:
            clauses.append("logger_name = %(to_ts)s")
            values['logger_name'] = logger_name

        columns, aliases = zip(*fields)
        query = QueryProcessor(tables=['scheduler_log_messages'],
                               columns=columns, aliases=aliases,
                               joins=['LEFT JOIN host ON host_id = host.id'],
                               clauses=clauses, values=values,
                               opts={'order': 'msg_time'})
        return query.execute()


class DBLogger(object):
    """DBLogger class for encapsulating scheduler logging. It is thread-safe
    as both logging parts do this per se (loggind + DB handler via context)"""

    def __init__(self, logger_name=None):
        self.log_level = None
        if logger_name:
            self.logger = logger_name
        else:
            self.logger = 'koji.scheduler'

    def log(self, msg, logger_name=None, level=logging.NOTSET,
            task_id=None, host_id=None, location=None):
        if self.log_level is None:
            # can't be done in constructor, as config is not loaded in that time
            log_level = context.opts.get('SchedulerLogLevel')
            valid_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
            if log_level not in valid_levels:
                raise koji.GenericError(f"Invalid log level: {log_level}")
            self.log_level = logging.getLevelName(log_level)
        if level < self.log_level:
            return
        if not logger_name:
            logger_name = self.logger
        if location is None:
            frame = inspect.currentframe()
            frames = inspect.getouterframes(frame)
            frame = frames[1]
            location = frame.function
        # log to regular log
        text = f"task: {task_id}, host: {host_id}, location: {location}, message: {msg}"
        logging.getLogger(logger_name).log(level, text)
        # log to db
        insert = InsertProcessor(
            'scheduler_log_messages',
            data={
                'logger_name': logger_name,
                'level': logging._levelToName[level],
                'task_id': task_id,
                'host_id': host_id,
                'location': location,
                'msg': msg,
            }
        )
        insert.execute()

    debug = functools.partialmethod(log, level=logging.DEBUG)
    info = functools.partialmethod(log, level=logging.INFO)
    warning = functools.partialmethod(log, level=logging.WARNING)
    error = functools.partialmethod(log, level=logging.ERROR)
    critical = functools.partialmethod(log, level=logging.CRITICAL)


dblogger = DBLogger()
