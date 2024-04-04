import datetime
import mock
import unittest

import koji
import kojihub
import kojihub.db
from kojihub import repos


QP = repos.QueryProcessor
IP = repos.InsertProcessor
UP = repos.UpdateProcessor
TASK = kojihub.Task


class MyError(Exception):
    pass


class BaseTest(unittest.TestCase):

    def setUp(self):
        self.context = mock.MagicMock()
        mock.patch('kojihub.repos.context', new=self.context).start()
        mock.patch('kojihub.db.context', new=self.context).start()
        mock.patch('kojihub.kojihub.context', new=self.context).start()
        self.context.opts = {
            # duplicating hub defaults
            'MaxRepoTasks': 10,
            'MaxRepoTasksMaven': 2,
            'RepoRetries': 3,
            'RequestCleanTime': 60 * 24,
            'RepoLag': 3600,
            'RepoAutoLag': 7200,
            'RepoLagWindow': 600,
            'RepoQueueUser': 'kojira',
            'DebuginfoTags': '',
            'SourceTags': '',
            'SeparateSourceTags': '',
        }

        self.db_lock = mock.patch('kojihub.repos.db_lock').start()
        self.db_lock.return_value = True

        self.QueryProcessor = mock.patch('kojihub.repos.QueryProcessor',
                                         side_effect=self.getQuery).start()
        self.queries = []
        self.InsertProcessor = mock.patch('kojihub.repos.InsertProcessor',
                                          side_effect=self.getInsert).start()
        self.inserts = []
        self.UpdateProcessor = mock.patch('kojihub.repos.UpdateProcessor',
                                          side_effect=self.getUpdate).start()
        self.updates = []
        self._dml = mock.patch('kojihub.db._dml').start()
        self.exports = kojihub.RootExports()
        self.get_tag = mock.patch('kojihub.kojihub.get_tag').start()
        self.get_id = mock.patch('kojihub.kojihub.get_id').start()
        self.make_task = mock.patch('kojihub.kojihub.make_task').start()
        self.query_executeOne = mock.MagicMock()

        self.RepoQueueQuery = mock.patch('kojihub.repos.RepoQueueQuery').start()

    def tearDown(self):
        mock.patch.stopall()

    def getQuery(self, *args, **kwargs):
        query = QP(*args, **kwargs)
        query.execute = mock.MagicMock()
        query.executeOne = self.query_executeOne
        self.queries.append(query)
        return query

    def getInsert(self, *args, **kwargs):
        insert = IP(*args, **kwargs)
        insert.execute = mock.MagicMock()
        self.inserts.append(insert)
        return insert

    def getUpdate(self, *args, **kwargs):
        update = UP(*args, **kwargs)
        update.execute = mock.MagicMock()
        self.updates.append(update)
        return update

    @mock.patch('kojihub.repos.clean_repo_queue')
    def test_check_queue(self, clean_repo_queue):
        self.db_lock.return_value = True
        repos.check_repo_queue()

    def test_clean_queue(self):
        repos.clean_repo_queue()

    def test_valid_repo(self):
        req = mock.MagicMock()
        repo = mock.MagicMock()
        repos.valid_repo(req, repo)

    def test_queue_task(self):
        req = {'id': 100, 'tag_id': 42, 'min_event': None, 'at_event': None, 'opts': None}
        req['opts'] = {}
        repos.repo_queue_task(req)
