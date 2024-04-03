from __future__ import absolute_import
import json
import mock
import os.path
import shutil
import tempfile
import time
import unittest

import koji

from . import loadkojira
kojira = loadkojira.kojira


class OurException(Exception):
    pass


class ManagedRepoTest(unittest.TestCase):

    def setUp(self):
        self.workdir = tempfile.mkdtemp()
        self.kojidir = self.workdir + '/koji'
        os.mkdir(self.kojidir)
        self.pathinfo = koji.PathInfo(self.kojidir)
        mock.patch.object(kojira, 'pathinfo', new=self.pathinfo, create=True).start()

        self.session = mock.MagicMock()
        self.options = mock.MagicMock()
        self.mgr = mock.MagicMock()
        self.unlink = mock.patch('os.unlink').start()
        self.data = {
            'create_event': 497359,
            'create_ts': 1709791593.368943,
            'creation_ts': 1709791593.367575,
            'dist': False,
            'end_event': None,
            'id': 2385,
            'opts': {'debuginfo': False, 'separate_src': False, 'src': False},
            'state': 1,
            'state_ts': 1710705227.166751,
            'tag_id': 50,
            'tag_name': 'some-tag',
            'task_id': 13290,
        }
        self.repo = self.mkrepo(self.data)

    def mkrepo(self, data):
        repodir = self.kojidir + ('/repos/%(tag_name)s/%(id)s' % self.data)
        os.makedirs(repodir)
        with open('%s/repo.json' % repodir, 'wt', encoding='utf-8') as fp:
            # technically not quite the right data, but close enough
            json.dump(data, fp, indent=2)
        for arch in ('x86_64', 'aarch64'):
            os.mkdir(repodir + '/' + arch)
        repo = kojira.ManagedRepo(self.mgr, data.copy())
        return repo

    def tearDown(self):
        mock.patch.stopall()
        shutil.rmtree(self.workdir)

    def test_get_info(self):
        info = self.repo.get_info()
        self.assertEqual(info, self.data)

    def test_get_path(self):
        path = self.repo.get_path()
        repodir = self.kojidir + ('/repos/%(tag_name)s/%(id)s' % self.repo.data)
        self.assertEqual(path, repodir)

