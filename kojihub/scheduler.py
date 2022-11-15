import functools
import logging

from koji.db import InsertProcessor


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
