from __future__ import absolute_import
import mock
import six
import unittest

from koji_cli.commands import handle_list_users
from . import utils


class TestListUsers(utils.CliTestCase):

    def setUp(self):
        self.error_format = """Usage: %s list-users [options]
(Specify the --help global option for a list of other help options)

%s: error: {message}
""" % (self.progname, self.progname)

        self.session = mock.MagicMock()
        self.activate_session_mock = mock.patch('koji_cli.commands.activate_session').start()
        self.options = mock.MagicMock()
        self.options.quiet = True
        self.users = [{'id': 101, 'name': "user01"},
                      {'id': 102, 'name': "user02"},
                      {'id': 103, 'name': "user03"},
                      {'id': 104, 'name': "user04"},
                      {'id': 105, 'name': "user05"}
        ]
        self.perm='admin'
                      

    def test_handle_list_users_arg_error(self):
        """Test handle_list_users argument error (no argument is required)"""
        expected = self.format_error_message("This command takes no arguments")
        self.assert_system_exit(
            handle_list_users,
            self.options,
            self.session,
            ['arg-1', 'arg-2'],
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_not_called()
        self.session.getPermsUser.assert_not_called()
    

    def test_handle_list_users_perm_not_exist(self):
        """Test handle_list_users when perm does not exist"""
        self.session.getPermsUser.return_value = []
        expected = self.format_error_message("No such permission: notperm")
        self.assert_system_exit(
            handle_list_users,
            self.options,
            self.session,
            ['--perm', 'notperm'],
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_called_once()
        self.session.getPermsUser.assert_called_once()

    @mock.patch('sys.stdout', new_callable=six.StringIO)
    def test_handle_list_users_no_perm(self, stdout):
        """Test handle_list_users when --perm is not setting"""
        expected = self.format_error_message("""Please provide a permission with --perm""")
        self.assert_system_exit(
            handle_list_users,
            self.options,
            self.session,
            [],
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_called_once()
        self.session.getPermsUser.assert_not_called()

    @mock.patch('sys.stdout', new_callable=six.StringIO)
    def test_handle_list_users_perm(self, stdout):
        """Test handle_list_users user permissions"""
        expected = "user01\nuser02\nuser03\nuser04\nuser05\n"
        users = [p['name'] for p in self.users[::1]]
        self.session.getPermsUser.return_value = users
        handle_list_users(self.options, self.session, ['--perm', self.perm])
        self.assert_console_message(stdout, expected)
        self.activate_session_mock.assert_called_once()
        self.session.getPermsUser.assert_called_once()

    def test_handle_list_users_help(self):
        self.assert_help(
            handle_list_users,
            """Usage: %s list-users [options]
(Specify the --help global option for a list of other help options)

Options:
  -h, --help   show this help message and exit
  --perm=PERM  List users that have a given permission
""" % self.progname)


if __name__ == '__main__':
    unittest.main()
