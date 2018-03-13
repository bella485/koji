from __future__ import absolute_import

import os
import unittest

import mock
import six

import koji_cli.lib
from . import loadcli

cli = loadcli.cli


class TestListCommands(unittest.TestCase):
    def setUp(self):
        self.options = mock.MagicMock()
        self.session = mock.MagicMock()
        self.args = mock.MagicMock()
        self.original_parser = koji_cli.lib.OptionParser
        koji_cli.lib.OptionParser = mock.MagicMock()
        self.parser = koji_cli.lib.OptionParser.return_value

    def tearDown(self):
        koji_cli.lib.OptionParser = self.original_parser

    # Show long diffs in error output...
    maxDiff = None

    @mock.patch('sys.stdout', new_callable=six.StringIO)
    def test_list_commands(self, stdout):
        koji_cli.lib.list_commands()
        actual = stdout.getvalue()
        if six.PY2:
            actual = actual.replace('nosetests', 'koji')
        else:
            actual = actual.replace('nosetests-3', 'koji')
        filename = os.path.dirname(__file__) + '/data/list-commands.txt'
        with open(filename, 'rb') as f:
            expected = f.read().decode('ascii')
        self.assertMultiLineEqual(actual, expected)

    @mock.patch('sys.stdout', new_callable=six.StringIO)
    def test_handle_admin_help(self, stdout):
        options, arguments = mock.MagicMock(), mock.MagicMock()
        options.admin = True
        self.parser.parse_args.return_value = [options, arguments]
        koji_cli.lib.CommandExports.handle_help(self.options, self.session, self.args)
        actual = stdout.getvalue()
        if six.PY2:
            actual = actual.replace('nosetests', 'koji')
        else:
            actual = actual.replace('nosetests-3', 'koji')
        filename = os.path.dirname(__file__) + '/data/list-commands-admin.txt'
        with open(filename, 'rb') as f:
            expected = f.read().decode('ascii')
        self.assertMultiLineEqual(actual, expected)
