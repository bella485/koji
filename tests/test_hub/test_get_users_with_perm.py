import mock
import unittest
import koji
import kojihub


class TestGetPermsUser(unittest.TestCase):
    def setUp(self):
        self.context = mock.patch('kojihub.kojihub.context').start()
        self.get_perm_id = mock.patch('kojihub.kojihub.get_perm_id').start()
        self.get_users_with_perm = mock.patch('kojihub.kojihub.get_users_with_perm').start()

    def tearDown(self):
        mock.patch.stopall()

    def test_no_admin(self):
        self.context.session.hasPerm.return_value = False
        with self.assertRaises(koji.ActionNotAllowed) as ex:
            kojihub.RootExports().getPermsUser('admin')
        self.assertEqual("This action requires admin privileges", str(ex.exception))
        self.get_users_with_perm.assert_not_called()

    def test_no_perm(self):
        self.get_perm_id.return_value = None
        with self.assertRaises(koji.GenericError):
            kojihub.RootExports().getPermsUser('noperm')
        self.get_users_with_perm.assert_not_called()

    def test_normal(self):
        self.get_perm_id.return_value = 1
        kojihub.RootExports().getPermsUser('admin')
        self.get_users_with_perm.assert_called_once_with(1)
