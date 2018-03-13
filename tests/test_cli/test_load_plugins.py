from __future__ import absolute_import

import os
import unittest

import mock

from koji_cli import lib


class TestLoadPlugins(unittest.TestCase):
    @mock.patch('logging.getLogger')
    def test_load_plugins(self, getLogger):
        options = mock.MagicMock()
        lib.load_plugins(options, [os.path.dirname(__file__) + '/data/plugins',
                                   os.path.dirname(
                                       __file__) + '/data/plugins2'])
        self.assertTrue(callable(lib.CommandExports.foobar))
        self.assertTrue(callable(lib.CommandExports.foo2))
        self.assertTrue(hasattr(lib.CommandExports, 'foo6'))
        self.assertFalse(hasattr(lib.CommandExports, 'foo3'))
        self.assertFalse(hasattr(lib.CommandExports, 'foo4'))
        self.assertFalse(hasattr(lib.CommandExports, 'foo5'))
        self.assertFalse(hasattr(lib.CommandExports, 'sth'))
