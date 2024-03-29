import mock

import koji
import kojihub
from .utils import DBQueryTestCase


class TestGetUserGroups(DBQueryTestCase):

    def setUp(self):
        super(TestGetUserGroups, self).setUp()
        self.context = mock.patch('kojihub.kojihub.context').start()
        self.context_db = mock.patch('kojihub.db.context').start()
        self.get_user = mock.patch('kojihub.kojihub.get_user').start()
        self.exports = kojihub.RootExports()

        aqp = mock.patch('kojihub.auth.QueryProcessor',
                         side_effect=self.get_query).start()
        self.AuthQueryProcessor = aqp

    def test_non_exist_group(self):
        user = 'test-user'
        self.get_user.return_value = {'id': 23, 'usertype': 2}
        with self.assertRaises(koji.GenericError) as cm:
            self.exports.getUserGroups(user)
        self.get_user.return_value = []
        with self.assertRaises(koji.GenericError) as cm:
            self.exports.getUserGroups(user)
        self.assertEqual("No such user: %s" % user, str(cm.exception))
        self.assertEqual(len(self.queries), 0)

    def test_valid(self):
        user = 'test-user'
        self.get_user.return_value = {'id': 23, 'usertype': 0}
        self.exports.getUserGroups(user)
        self.assertEqual(len(self.queries), 1)
        query = self.queries[0]
        self.assertEqual(query.tables, ['user_groups'])
        self.assertEqual(query.joins, ['users ON group_id = users.id'])
        self.assertEqual(query.clauses, ['active IS TRUE',
                                         'user_id=%(user_id)i',
                                         'users.usertype=%(t_group)i'])
        self.assertEqual(query.values, {'t_group': 2,
                                        'user_id': 23})
        self.assertEqual(query.columns, ['group_id', 'name'])
