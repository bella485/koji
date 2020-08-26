import io
import json
import mock
import os
import sys
import unittest


from nose.plugins.skip import SkipTest

from koji_cli.commands import handle_import_comps
from . import utils

class TestImportComps(utils.CliTestCase):
    # Show long diffs in error output...
    maxDiff = None

    @mock.patch('sys.stdout', new_callable=io.StringIO)
    @mock.patch('koji_cli.commands.libcomps')
    @mock.patch('koji_cli.commands.activate_session')
    def test_handle_import_comps_libcomps(
            self,
            mock_activate_session,
            mock_libcomps,
            stdout):
        filename = './data/comps-example.xml'
        tag = 'tag'
        tag_info = {'name': tag, 'id': 1}
        force = None
        args = [filename, tag]
        kwargs = {'force': force}
        options = mock.MagicMock()

        # Mock out the xmlrpc server
        session = mock.MagicMock()
        session.getTag.return_value = tag_info

        # Run it and check immediate output
        # args: ./data/comps-example.xml, tag
        # expected: success
        rv = handle_import_comps(options, session, args)
        actual = stdout.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)

        # Finally, assert that things were called as we expected.
        mock_activate_session.assert_called_once_with(session, options)
        session.getTag.assert_called_once_with(tag)
        self.assertNotEqual(rv, 1)

    @mock.patch('sys.stderr', new_callable=io.StringIO)
    @mock.patch('koji_cli.commands.activate_session')
    def test_handle_import_comps_tag_not_exists(
            self,
            mock_activate_session,
            stderr):
        filename = './data/comps-example.xml'
        tag = 'tag'
        tag_info = None
        args = [filename, tag]
        options = mock.MagicMock()

        # Mock out the xmlrpc server
        session = mock.MagicMock()
        session.getTag.return_value = tag_info

        # Run it and check immediate output
        # args: ./data/comps-example.xml, tag
        # expected: failed: tag does not exist
        with self.assertRaises(SystemExit) as ex:
            handle_import_comps(options, session, args)
        self.assertExitCode(ex, 1)
        actual = stderr.getvalue()
        expected = 'No such tag: tag\n'
        self.assertMultiLineEqual(actual, expected)

        # Finally, assert that things were called as we expected.
        mock_activate_session.assert_called_once_with(session, options)
        session.getTag.assert_called_once_with(tag)

    @mock.patch('sys.stdout', new_callable=io.StringIO)
    @mock.patch('sys.stderr', new_callable=io.StringIO)
    @mock.patch('koji_cli.commands.activate_session')
    def test_handle_import_comps_help(
            self,
            mock_activate_session,
            stderr,
            stdout):
        args = []
        progname = os.path.basename(sys.argv[0]) or 'koji'
        options = mock.MagicMock()

        # Mock out the xmlrpc server
        session = mock.MagicMock()

        # Run it and check immediate output
        with self.assertRaises(SystemExit) as ex:
            handle_import_comps(options, session, args)
        self.assertExitCode(ex, 2)
        actual_stdout = stdout.getvalue()
        actual_stderr = stderr.getvalue()
        expected_stdout = ''
        expected_stderr = """Usage: %s import-comps [options] <file> <tag>
(Specify the --help global option for a list of other help options)

%s: error: Incorrect number of arguments
""" % (progname, progname)
        self.assertMultiLineEqual(actual_stdout, expected_stdout)
        self.assertMultiLineEqual(actual_stderr, expected_stderr)

        # Finally, assert that things were called as we expected.
        mock_activate_session.assert_not_called()
        session.getTag.assert_not_called()
        session.getTagGroups.assert_not_called()
        session.groupListAdd.assert_not_called()

    def _test_import_comps(
            self,
            method,
            comps_file,
            stdout_file,
            calls_file,
            stdout):
        tag = 'tag'
        options = mock.MagicMock()
        options.force = None

        # Mock out the xmlrpc server
        session = mock.MagicMock()

        # Run it and check immediate output
        # args: comps.xml, tag
        # expected: success
        rv = method.__call__(session, comps_file, tag, options)
        expected = ''
        with open(stdout_file, 'rb') as f:
            expected = f.read().decode('ascii')
        self.assertMultiLineEqual(stdout.getvalue(), expected)

        # compare mock_calls stored as json
        expected = []
        for c in json.load(open(calls_file, 'rt')):
            expected.append(getattr(mock.call, c[0]).__call__(*c[1], **c[2]))

        if hasattr(session, 'assertHasCalls'):
            session.assertHasCalls(expected)
        else:
            session.assert_has_calls(expected)
        self.assertNotEqual(rv, 1)


'''
def _generate_out_calls(method, comps_file, stdout_file, calls_file):
    tag = 'tag'
    force = None
    options = {'force': force}

    # Mock out the xmlrpc server
    session = mock.MagicMock()

    with open(stdout_file, 'wb') as f:
        # redirect stdout to stdout_file
        orig_stdout = sys.stdout
        sys.stdout = f
        # args: comps.xml, tag
        # expected: success
        method.__call__(session, comps_file, tag, options)
        sys.stdout = orig_stdout
    with open(calls_file, 'wb') as f:
        f.write(str(session.mock_calls).encode('ascii') + '\n')


def generate_out_calls():
    """Generate .out and .calls files for tests.
    These files should be carefully check to make sure they're excepted"""
    path = os.path.dirname(__file__)

    comps_file = path + '/data/comps-example.xml'
    stdout_file = path + '/data/comps-example.libcomps.out'
    calls_file = path + '/data/comps-example.libcomps.calls'
    _generate_out_calls(_import_comps, comps_file, stdout_file, calls_file)

    comps_file = path + '/data/comps-sample.xml'
    stdout_file = path + '/data/comps-sample.libcomps.out'
    calls_file = path + '/data/comps-sample.libcomps.calls'
    _generate_out_calls(_import_comps, comps_file, stdout_file, calls_file)
'''


if __name__ == '__main__':
    unittest.main()
