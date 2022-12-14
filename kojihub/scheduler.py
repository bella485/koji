import functools
import logging

import koji
from koji.db import InsertProcessor, QueryProcessor, DeleteProcessor

logger = logging.getLogger('koji.scheduler')


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
        opts={'order': 'id'}
    )

    return query.execute()


def get_task_runs(taskID=None, hostID=None, states=None):
    """Return content of scheduler queue

    :param int taskID: filter by task
    :param int hostID: filter by host
    :param list[int] states: filter by states
    :returns list[dict]: list of dicts
    """

    columns = ['task_id', 'host_id', 'state', 'create_time', 'start_time', 'end_time']
    clauses = []
    if taskID is not None:
        clauses.append('task_id = %(taskID)i')
    if hostID is not None:
        clauses.append('host_id = %(hostID)i')
    if states is not None:
        clauses.append('states IN %(states)s')

    query = QueryProcessor(
        tables=['scheduler_task_runs'], columns=columns,
        clauses=clauses, values=locals()
    )
    return query.execute()


def schedule(task_id=None):
    """Run scheduler"""

    # stupid for now, just add new task to first builder
    query = QueryProcessor(
        tables=['host'],
        columns=['id'],
        joins=['host_config ON host.id=host_config.host_id'],
        clauses=['enabled IS TRUE'],
        opts={'limit': 1}
    )
    logger.error('xxxxxxxxxxxxxxx %s', str(query))
    host = query.executeOne()
    if not host:
        return

    insert = InsertProcessor(
        table='scheduler_task_runs',
        data={
            'task_id': task_id,
            'host_id': host['id'],
            'state': koji.TASK_STATES['SCHEDULED'],
        }
    )
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
            ('logger_name', 'logger_name'),
            ('level', 'level'),
            ('location', 'location'),
            ('msg', 'msg'),
            ('hosts.name', 'host_name'),
        )
        clauses = []
        values = {}
        if taskID is not None:
            clauses.append("taskID = %(taskID)")
            values['taskID'] = taskID
        if hostID is not None:
            clauses.append("hostID = %(hostID)")
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
                               joins=['hosts ON host_id = hosts.id'],
                               clauses=clauses, values=values,
                               opts={'order': 'msg_time'})
        return query.execute()


class DBLogger(object):
    """DBLogger class for encapsulating scheduler logging. It is thread-safe
    as both logging parts do this per se (loggind + DB handler via context)"""

    def __init__(self, logger_name=None):
        if logger_name:
            self.logger = logger_name
        else:
            self.logger = 'koji.scheduler'

    def log(self, msg, logger_name=None, level=logging.NOTSET,
            task_id=None, host_id=None, location=None):
        if not logger_name:
            logger_name = self.logger
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
