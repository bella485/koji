from __future__ import absolute_import
import mock
import unittest

from koji_cli.commands.add_group_req import handle_add_group_req
from . import utils


class TestAddGroupReq(utils.CliTestCase):

    # Show long diffs in error output...
    maxDiff = None

    def setUp(self):
        self.session = mock.MagicMock()
        self.options = mock.MagicMock()
        self.activate_session = mock.patch('koji_cli.commands.add_group_req.activate_session').start()

        self.error_format = """Usage: %s add-group-req [options] <tag> <target group> <required group>
(Specify the --help global option for a list of other help options)

%s: error: {message}
""" % (self.progname, self.progname)

    def tearDown(self):
        mock.patch.stopall()

    def test_handle_add_group_req(self):
        """Test handle_add_group_req function"""
        arguments = ['fedora-build', 'build', 'srpm-build']
        handle_add_group_req(self.options, self.session, arguments)
        self.session.groupReqListAdd.assert_called_with(*arguments)
        self.activate_session.assert_called_with(self.session, self.options)

    def test_handle_add_group_req_argument_error(self):
        """Test handle_add_group_req function with wrong argument"""
        expected = self.format_error_message(
            "You must specify a tag name and two group names")
        for arg in [[], ['tag'], ['tag', 'grp', 'opt1', 'opt2']]:
            self.assert_system_exit(
                handle_add_group_req,
                self.options,
                self.session,
                arg,
                stderr=expected,
                activate_session=None)
        self.activate_session.assert_not_called()

    def test_handle_add_group_req_help(self):
        self.assert_help(
            handle_add_group_req,
            """Usage: %s add-group-req [options] <tag> <target group> <required group>
(Specify the --help global option for a list of other help options)

Options:
  -h, --help  show this help message and exit
""" % self.progname)


if __name__ == '__main__':
    unittest.main()
