from __future__ import absolute_import

import mock
from mock import call

import koji
from koji_cli.commands import handle_edit_tag_inheritance
from . import utils


class TestEditTagInheritance(utils.CliTestCase):
    def setUp(self):
        self.options = mock.MagicMock()
        self.options.debug = False
        self.session = mock.MagicMock()
        self.session.getAPIVersion.return_value = koji.API_VERSION
        self.activate_session_mock = mock.patch('koji_cli.commands.activate_session').start()
        self.error_format = """Usage: %s edit-tag-inheritance [options] <tag> <parent> <priority>
(Specify the --help global option for a list of other help options)

%s: error: {message}
""" % (self.progname, self.progname)
        self.tag = 'test-tag'
        self.parent_tag = 'parent-test-tag'
        self.priority = '99'
        self.new_priority = '15'
        self.tag_inheritance = {'child_id': 1,
                                'intransitive': False,
                                'maxdepth': None,
                                'name': self.tag,
                                'noconfig': False,
                                'parent_id': 2,
                                'pkg_filter': '',
                                'priority': self.priority}
        self.child_tag_info = {'arches': 'x86_64',
                               'extra': {},
                               'id': 1,
                               'locked': False,
                               'maven_include_all': False,
                               'maven_support': False,
                               'name': 'test-tag',
                               'perm': None,
                               'perm_id': None}
        self.parent_tag_info = {'arches': 'x86_64',
                                'extra': {},
                                'id': 2,
                                'locked': False,
                                'maven_include_all': False,
                                'maven_support': False,
                                'name': 'parent-test-tag',
                                'perm': None,
                                'perm_id': None}

    def test_edit_tag_inheritance_without_option(self):
        expected = self.format_error_message(
            "This command takes at least one argument: a tag name or ID")
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [],
            stdout='',
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_not_called()
        self.session.getTag.assert_not_called()
        self.session.getInheritanceData.assert_not_called()
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_non_exist_tag(self):
        self.session.getTag.return_value = None
        expected = self.format_error_message("No such tag: %s" % self.tag)
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag, self.parent_tag, self.priority],
            stdout='',
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(self.tag)
        self.session.getInheritanceData.assert_not_called()
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_non_exist_parent_tag(self):
        self.session.getTag.side_effect = [self.child_tag_info, None]
        expected = self.format_error_message("No such tag: %s" % self.parent_tag)
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag, self.parent_tag, self.priority],
            stdout='',
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_has_calls([call(self.tag), call(self.parent_tag)])
        self.session.getInheritanceData.assert_not_called()
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_more_arguments(self):
        expected = self.format_error_message(
            "This command takes at most three argument: a tag name or ID, "
            "a parent tag name or ID, and a priority")
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            ['arg1', 'arg2', 'arg3', 'arg4'],
            stdout='',
            stderr=expected,
            activate_session=None,
            exit_code=2
        )
        self.activate_session_mock.assert_not_called()
        self.session.getTag.assert_not_called()
        self.session.getInheritanceData.assert_not_called()
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_non_exist_inheritance(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.return_value = []
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag, self.parent_tag, self.priority],
            stdout='',
            stderr='No inheritance link found to remove.  Please check your arguments\n',
            activate_session=None,
            exit_code=1
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_has_calls([call(self.tag), call(self.parent_tag)])
        self.session.getInheritanceData.assert_called_once_with(1)
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_multi_inheritance_without_parent(self):
        self.session.getTag.return_value = self.child_tag_info
        self.session.getInheritanceData.return_value = [self.tag_inheritance, self.tag_inheritance]
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag],
            stdout='Multiple matches for tag.\n',
            stderr='Please specify a parent on the command line.\n',
            activate_session=None,
            exit_code=1
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(self.tag)
        self.session.getInheritanceData.assert_called_once_with(1)
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_multi_inheritance_without_priority(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.return_value = [self.tag_inheritance, self.tag_inheritance]
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag, self.parent_tag],
            stdout='Multiple matches for tag.\n',
            stderr='Please specify a priority on the command line.\n',
            activate_session=None,
            exit_code=1
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_has_calls([call(self.tag), call(self.parent_tag)])
        self.session.getInheritanceData.assert_called_once_with(1)
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_multi_inheritance(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.return_value = [self.tag_inheritance, self.tag_inheritance]
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag, self.parent_tag, self.priority],
            stdout='Multiple matches for tag.\n',
            stderr='Error: Key constraints may be broken.  Exiting.\n',
            activate_session=None,
            exit_code=1
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_has_calls([call(self.tag), call(self.parent_tag)])
        self.session.getInheritanceData.assert_called_once_with(1)
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_already_active_inheritance(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.side_effect = [[self.tag_inheritance],
                                                       [self.tag_inheritance]]
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            [self.tag, self.parent_tag, self.priority, '--priority', self.priority],
            stdout='',
            stderr='Error: There is already an active inheritance with that priority on %s, '
                   'please specify a different priority with --priority.\n' % self.tag,
            activate_session=None,
            exit_code=1
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_has_calls([call(self.tag), call(self.parent_tag)])
        self.session.getInheritanceData.assert_has_calls([call(1), call(1)])
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_valid_maxdepth_digit(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.side_effect = [[self.tag_inheritance],
                                                       [self.tag_inheritance]]
        handle_edit_tag_inheritance(self.options, self.session,
                                    ['--priority', self.new_priority, '--maxdepth', '123',
                                     '--intransitive', '--noconfig', '--pkg-filter',
                                     self.tag, self.parent_tag])
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(self.parent_tag)
        self.session.getInheritanceData.assert_has_calls([call(1), call(1)])
        self.session.setInheritanceData.assert_called_once_with(
            1, [{'child_id': 1, 'intransitive': True, 'maxdepth': 123, 'name': self.tag,
                 'noconfig': True, 'parent_id': 2, 'pkg_filter': self.tag,
                 'priority': int(self.new_priority)}])

    def test_edit_tag_inheritance_valid_maxdepth_none(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.side_effect = [[self.tag_inheritance],
                                                       [self.tag_inheritance]]
        handle_edit_tag_inheritance(self.options, self.session,
                                    ['--priority', self.new_priority, '--maxdepth', "None",
                                     '--intransitive', '--noconfig', '--pkg-filter',
                                     self.tag, self.parent_tag])
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(self.parent_tag)
        self.session.getInheritanceData.assert_has_calls([call(1), call(1)])
        self.session.setInheritanceData.assert_called_once_with(
            1, [{'child_id': 1, 'intransitive': True, 'maxdepth': None, 'name': self.tag,
                 'noconfig': True, 'parent_id': 2, 'pkg_filter': self.tag,
                 'priority': int(self.new_priority)}])

    def test_edit_tag_inheritance_valid_maxdepth_invalid(self):
        self.session.getTag.side_effect = [self.child_tag_info, self.parent_tag_info]
        self.session.getInheritanceData.side_effect = [[self.tag_inheritance],
                                                       [self.tag_inheritance]]
        self.assert_system_exit(
            handle_edit_tag_inheritance,
            self.options,
            self.session,
            ['--priority', self.new_priority, '--maxdepth', 'wrong-maxdepth', '--intransitive',
             '--noconfig', '--pkg-filter', self.tag, self.parent_tag],
            stdout='',
            stderr='Invalid maxdepth: wrong-maxdepth\n',
            activate_session=None,
            exit_code=1
        )
        self.activate_session_mock.assert_called_once_with(self.session, self.options)
        self.session.getTag.assert_called_once_with(self.parent_tag)
        self.session.getInheritanceData.assert_has_calls([call(1), call(1)])
        self.session.setInheritanceData.assert_not_called()

    def test_edit_tag_inheritance_help(self):
        self.assert_help(
            handle_edit_tag_inheritance,
            """Usage: %s edit-tag-inheritance [options] <tag> <parent> <priority>
(Specify the --help global option for a list of other help options)

Options:
  -h, --help            show this help message and exit
  --priority=PRIORITY   Specify a new priority
  --maxdepth=MAXDEPTH   Specify max depth
  --intransitive        Set intransitive
  --noconfig            Set to packages only
  --pkg-filter=PKG_FILTER
                        Specify the package filter
""" % self.progname)
