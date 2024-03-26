from __future__ import absolute_import
import mock
import time
import unittest

import koji

from . import loadkojira
kojira = loadkojira.kojira


class OurException(Exception):
    pass


class RepoManagerTest(unittest.TestCase):

    def setUp(self):
        self.session = mock.MagicMock()
        self.options = mock.MagicMock()
        self.mgr = kojira.RepoManager(self.options, self.session)

    def tearDown(self):
        mock.patch.stopall()

    @mock.patch('time.sleep')
    def test_regen_loop(self, sleep):
        subsession = mock.MagicMock()
        self.mgr.regenRepos = mock.MagicMock()
        self.mgr.regenRepos.side_effect = [None] * 10 + [OurException()]
        # we need the exception to terminate the infinite loop

        with self.assertRaises(OurException):
            self.mgr.regenLoop(subsession)

        self.assertEqual(self.mgr.regenRepos.call_count, 11)
        subsession.logout.assert_called_once()


# the end
