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
        self.context.session.assertLogin = mock.MagicMock()
        self.getLastEvent = mock.MagicMock()
        self.getEvent = mock.MagicMock()
        self.context.handlers = {
            'getLastEvent': self.getLastEvent,
            'getEvent': self.getEvent,
        }
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
        self.tag_last_change_event = mock.patch('kojihub.kojihub.tag_last_change_event').start()
        self.query_executeOne = mock.MagicMock()

        self.RepoQueueQuery = mock.patch('kojihub.repos.RepoQueueQuery').start()
        self.nextval = mock.patch('kojihub.repos.nextval').start()

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

    def test_hook(self):
        repos.repo_done_hook(100)

    def test_auto_req(self):
        repos.do_auto_requests()

    def test_get_repo(self):
        repos.get_repo('TAGID')

    def test_request(self):
        self.get_tag.return_value = {'id': 100, 'name': 'TAG', 'extra': {}}
        self.getLastEvent.return_value = {'id': 101010}
        self.tag_last_change_event.return_value = 100000
        repos.request_repo('TAGID')

    @mock.patch('kojihub.repos.get_repo')
    def test_request_existing_repo(self, get_repo):
        # if a matching repo exists, we should return it
        get_repo.return_value = 'MY-REPO'
        self.get_tag.return_value = {'id': 100, 'name': 'TAG', 'extra': {}}

        result = repos.request_repo('TAGID', min_event=101010)

        self.assertEqual(result['repo'], 'MY-REPO')
        get_repo.assert_called_with(100, min_event=101010, at_event=None, opts={})
        self.RepoQueueQuery.assert_not_called()
        self.nextval.assert_not_called()
        self.assertEqual(self.inserts, [])

    @mock.patch('kojihub.repos.get_repo')
    def test_request_existing_req(self, get_repo):
        # if a matching request exists, we should return it
        self.get_tag.return_value = {'id': 100, 'name': 'TAG', 'extra': {}}
        get_repo.return_value = None
        req = {'repo_id': None, 'sentinel': 'hello'}
        self.RepoQueueQuery.return_value.execute.return_value = [req]

        result = repos.request_repo('TAG', min_event=101010)

        self.assertEqual(result['request'], req)
        get_repo.assert_called_with(100, min_event=101010, at_event=None, opts={})
        self.RepoQueueQuery.assert_called_once()
        expect = [['tag_id', '=', 100],
                  ['active', 'IS', True],
                  ['opts', '=', '{}'],
                  ['min_event', '>=', 101010]]
        clauses = self.RepoQueueQuery.mock_calls[0][1][0]
        self.assertEqual(clauses, expect)
        self.nextval.assert_not_called()
        self.assertEqual(self.inserts, [])

    @mock.patch('kojihub.repos.get_repo')
    def test_request_new_req(self, get_repo):
        # if a matching request exists, we should return it
        self.get_tag.return_value = {'id': 100, 'name': 'TAG', 'extra': {}}
        get_repo.return_value = None
        self.RepoQueueQuery.return_value.execute.return_value = []
        self.RepoQueueQuery.return_value.executeOne.return_value = 'NEW-REQ'
        self.nextval.return_value = 'NEW-ID'

        result = repos.request_repo('TAG', min_event=101010)

        get_repo.assert_called_with(100, min_event=101010, at_event=None, opts={})
        self.assertEqual(len(self.inserts), 1)
        expect = {
            'id': 'NEW-ID',
            'tag_id': 100,
            'at_event': None,
            'min_event': 101010,
            'opts': '{}',
        }
        self.assertEqual(self.inserts[0].data, expect)
        self.assertEqual(self.RepoQueueQuery.call_count, 2)
        # clauses for final query
        clauses = self.RepoQueueQuery.call_args[1]['clauses']
        self.assertEqual(clauses, [['id', '=', 'NEW-ID']])
        self.assertEqual(result['request'], 'NEW-REQ')

    @mock.patch('kojihub.repos.get_repo')
    def test_request_at_event(self, get_repo):
        # similate an at_event request that finds an existing matching request to return
        self.get_tag.return_value = {'id': 100, 'name': 'TAG', 'extra': {}}
        get_repo.return_value = None
        req = {'repo_id': None, 'sentinel': 'hello'}
        self.RepoQueueQuery.return_value.execute.return_value = [req]

        result = repos.request_repo('TAG', at_event=101010)

        self.assertEqual(result['request'], req)
        get_repo.assert_called_with(100, min_event=None, at_event=101010, opts={})
        self.RepoQueueQuery.assert_called_once()
        expect = [['tag_id', '=', 100],
                  ['active', 'IS', True],
                  ['opts', '=', '{}'],
                  ['at_event', '=', 101010]]
        clauses = self.RepoQueueQuery.mock_calls[0][1][0]
        self.assertEqual(clauses, expect)
        self.nextval.assert_not_called()
        self.assertEqual(self.inserts, [])

    def test_check_req(self):
        repos.check_repo_request(99)
