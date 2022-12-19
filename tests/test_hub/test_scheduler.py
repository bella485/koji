import logging
import mock
import unittest

import scheduler

IP = scheduler.InsertProcessor


class TestDBLogger(unittest.TestCase):
    def setUp(self):
        self.InsertProcessor = mock.patch('scheduler.InsertProcessor',
                                          side_effect=self.getInsert).start()
        self.inserts = []

    def tearDown(self):
        mock.patch.stopall()

    def getInsert(self, *args, **kwargs):
        insert = IP(*args, **kwargs)
        insert.execute = mock.MagicMock()
        self.inserts.append(insert)
        return insert

    def test_defaults(self):
        logger = scheduler.DBLogger()
        self.assertEqual(logger.logger, 'koji.scheduler')
        self.assertEqual(len(self.inserts), 0)

    def test_basic(self):
        logger = scheduler.DBLogger()
        logger.log("text")
        self.assertEqual(len(self.inserts), 1)
        insert = self.inserts[0]
        self.assertEqual(insert.table, 'scheduler_log_messages')
        self.assertEqual(insert.data, {
            'task_id': None,
            'host_id': None,
            'logger_name': 'koji.scheduler',
            'level': 'NOTSET',
            'location': 'test_basic',
            'msg': 'text',
        })

    def test_all(self):
        logger = scheduler.DBLogger()
        logger.log("text", logger_name="logger_name", level=logging.ERROR,
                   task_id=123, host_id=456, location="location")
        self.assertEqual(len(self.inserts), 1)
        insert = self.inserts[0]
        self.assertEqual(insert.data, {
            'task_id': 123,
            'host_id': 456,
            'logger_name': 'logger_name',
            'level': 'ERROR',
            'location': 'location',
            'msg': 'text',
        })

    def test_levels(self):
        logger = scheduler.DBLogger()
        for level in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            m = getattr(logger, level.lower())
            m("")
            insert = self.inserts[0]
            self.assertEqual(insert.data['level'], level)
            self.inserts = []


class TestHostHashTable(unittest.TestCase):
    def test_get(self):
        hosts = [
            {
                'id': 1,
                'arches': ['i386', 'x86_64', 'noarch'],
                'channels': [1],
                'capacity': 2.0,
                'task_load': 0.0,
            },
            {
                'id': 2,
                'arches': ['i386'],
                'channels': [1, 2],
                'capacity': 2.0,
                'task_load': 0.0,
            },
            {
                'id': 3,
                'arches': ['x86_64', 'noarch'],
                'channels': [2],
                'capacity': 3.0,
                'task_load': 0.0,
            }
        ]
        ht = scheduler.HostHashTable(hosts)

        result = ht.get({'arch': 'noarch'})
        self.assertEqual(result['id'], 3)

        result = ht.get({'arch': 'x86_64'})
        self.assertEqual(result['id'], 3)

        result = ht.get({'channel_id': 2})
        self.assertEqual(result['id'], 3)

        result = ht.get({'channel_id': 2, 'arch': 'i386'})
        self.assertEqual(result['id'], 2)
