import functools
import logging

from koji.db import InsertProcessor, QueryProcessor


class SchedulerExports():
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
