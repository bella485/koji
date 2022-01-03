from __future__ import absolute_import

import mock
import six

from koji_cli.commands import handle_remove_group_pkg

import koji
from . import utils


class TestRemoveGroupPkg(utils.CliTestCase):

    def setUp(self):
        # Show long diffs in error output...
        self.maxDiff = None
        self.options = mock.MagicMock()
        self.options.debug = False
        self.session = mock.MagicMock()
        self.session.getAPIVersion.return_value = koji.API_VERSION
        self.activate_session_mock = mock.patch('koji_cli.commands.activate_session').start()
        self.error_format = """Usage: %s remove-group-pkg [options] <tag> <group> <pkg> [<pkg> ...]
(Specify the --help global option for a list of other help options)

%s: error: {message}
""" % (self.progname, self.progname)

    def test_handle_remove_pkg_not_existing_tag(self):
        tag = 'tag'
        package = 'package'
        group = 'group'
        args = [tag, group, package]

        self.session.getTag.return_value = None
        self.assert_system_exit(
            handle_remove_group_pkg,
            self.options, self.session, args,
            stderr='No such tag: %s\n' % tag,
            stdout='',
            activate_session=None,
            exit_code=1)
        # Finally, assert that things were called as we expected.
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(tag)
        self.session.groupPackageListRemove.assert_not_called()
        self.session.multiCall.assert_not_called()

    def test_handle_remove_pkg_wrong_count_args(self):
        tag = 'tag'
        group = 'group'
        args = [tag, group]
        expected_error = self.format_error_message(
            'You must specify a tag name, group name, and one or more package names')
        self.assert_system_exit(
            handle_remove_group_pkg,
            self.options, self.session, args,
            stderr=expected_error,
            stdout='',
            activate_session=None,
            exit_code=2)
        # Finally, assert that things were called as we expected.
        self.activate_session_mock.assert_not_called()
        self.session.getTag.assert_not_called()
        self.session.groupPackageListRemove.assert_not_called()
        self.session.multiCall.assert_not_called()

    @mock.patch('sys.stdout', new_callable=six.StringIO)
    @mock.patch('sys.stderr', new_callable=six.StringIO)
    def test_handle_remove_pkg(self, stderr, stdout):
        tag = 'tag'
        dsttag = {'name': tag, 'id': 1}
        package = 'package'
        group = 'group'
        args = [tag, group, package]

        self.session.getTag.return_value = dsttag
        self.session.groupPackageListRemove.return_value = None
        handle_remove_group_pkg(self.options, self.session, args)
        actual = stderr.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)
        actual = stdout.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)
        # Finally, assert that things were called as we expected.
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(tag)

    @mock.patch('sys.stdout', new_callable=six.StringIO)
    @mock.patch('sys.stderr', new_callable=six.StringIO)
    def test_handle_remove_pkg_with_force(self, stderr, stdout):
        tag = 'tag'
        dsttag = {'name': tag, 'id': 1}
        package = 'package'
        group = 'group'
        args = [tag, group, package, '--force']

        self.session.getTag.return_value = dsttag
        self.session.groupPackageListRemove.return_value = None
        handle_remove_group_pkg(self.options, self.session, args)
        actual = stderr.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)
        actual = stdout.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)
        # Finally, assert that things were called as we expected.
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(tag)
