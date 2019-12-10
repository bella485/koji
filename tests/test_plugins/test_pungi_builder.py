from __future__ import absolute_import
import copy
import mock
from nose.tools import raises
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import six.moves.configparser

# inject builder data
from tests.test_builder.loadkojid import kojid
import __main__
__main__.BuildRoot = kojid.BuildRoot

import koji
import pungi


class TestHandler(unittest.TestCase):
    def setUp(self):
        self.session = mock.MagicMock()
        self.session.host.taskWait.return_value = [{}, {}]
        self.session.host.subtask.return_value = 124

        self.br = mock.MagicMock()
        self.br.mock.return_value = 0
        self.br.id = 678
        self.br.rootdir.return_value = '/rootdir'
        pungi.BuildRoot = mock.MagicMock()
        pungi.BuildRoot.return_value = self.br

        options = mock.MagicMock()
        options.workdir = '/tmp/nonexistentdirectory'
        options.topurls = None
        self.t = pungi.PungiBuildinstallTask(123, 'pungi_buildinstall', {}, self.session, options)
        self.t.wait = mock.MagicMock()
        self.t.wait.return_value = {124: {'brootid': 2342345}}
        self.t.uploadTree = mock.MagicMock()

    def tearDown(self):
        self.t.removeWorkdir()
        pungi.BuildRoot = kojid.BuildRoot

    def test_handler_simple(self):
        self.t.handler('tag_name', 'noarch', packages=[], mounts=["/mnt/koji"], weight=10.0,
                       lorax_args={"sources": ['https://foo/', 'http://bar/'], "variant": "foo"})
        lorax_cmd = (
            'lorax --source=https://foo/ --source=http://bar/ --variant=foo '
            '--logfile=/tmp/nonexistentdirectory/tasks/123/123/logs/lorax.log '
            '/tmp/nonexistentdirectory/tasks/123/123/results')
        self.session.host.subtask.assert_called_once_with(
            arglist=['tag_name', 'noarch', lorax_cmd],
            kwargs={'mounts': ['/mnt/koji'], 'packages': ['lorax']}, method='runroot', parent=123)

    @raises(koji.GenericError)
    def test_handler_not_allowed_arg(self):
        self.t.handler('tag_name', 'noarch', lorax_args={"force": True})

    def test_handler_quoting_str_arg(self):
        self.t.handler(
            'tag_name', 'noarch', lorax_args={"variant": "foo; echo 1;\" echo 2;\' echo 3;"})
        lorax_cmd = (
            'lorax --variant=\'foo; echo 1;" echo 2;\'"\'"\' echo 3;\' '
            '--logfile=/tmp/nonexistentdirectory/tasks/123/123/logs/lorax.log '
            '/tmp/nonexistentdirectory/tasks/123/123/results')
        self.session.host.subtask.assert_called_once_with(
            arglist=['tag_name', 'noarch', lorax_cmd],
            kwargs={'mounts': [], 'packages': ['lorax']}, method='runroot', parent=123)

    def test_handler_quoting_extra_list_args(self):
        for opt in ["source", "dracut-arg"]:
            # + "s" to get the plural form - "sources" and "dracut_args"
            lorax_args = {opt + "s": ["foo; echo 1;\" echo 2;\' echo 3;"]}
        self.t.handler('tag_name', 'noarch', lorax_args=lorax_args)
        lorax_cmd = (
            'lorax --%s=\'foo; echo 1;" echo 2;\'"\'"\' echo 3;\' '
            '--logfile=/tmp/nonexistentdirectory/tasks/123/123/logs/lorax.log '
            '/tmp/nonexistentdirectory/tasks/123/123/results' % opt)
        self.session.host.subtask.assert_called_once_with(
            arglist=['tag_name', 'noarch', lorax_cmd],
            kwargs={'mounts': [], 'packages': ['lorax']}, method='runroot', parent=123)

    def test_handler_quoting_list_args(self):
        self.t.handler(
            'tag_name', 'noarch', lorax_args={"installpkgs": ["foo; echo 1;\" echo 2;\' echo 3;"]})
        lorax_cmd = (
            'lorax --installpkgs=\'foo; echo 1;" echo 2;\'"\'"\' echo 3;\' '
            '--logfile=/tmp/nonexistentdirectory/tasks/123/123/logs/lorax.log '
            '/tmp/nonexistentdirectory/tasks/123/123/results')
        self.session.host.subtask.assert_called_once_with(
            arglist=['tag_name', 'noarch', lorax_cmd],
            kwargs={'mounts': [], 'packages': ['lorax']}, method='runroot', parent=123)

    def test_handler_unset_bool_arg(self):
        self.t.handler('tag_name', 'noarch', lorax_args={"isfinal": False})
        lorax_cmd = (
            'lorax --logfile=/tmp/nonexistentdirectory/tasks/123/123/logs/lorax.log '
            '/tmp/nonexistentdirectory/tasks/123/123/results')
        self.session.host.subtask.assert_called_once_with(
            arglist=['tag_name', 'noarch', lorax_cmd],
            kwargs={'mounts': [], 'packages': ['lorax']}, method='runroot', parent=123)

    def test_handler_chown_uid(self):
        self.t.handler('tag_name', 'noarch', packages=[], mounts=["/mnt/koji"], weight=10.0,
                       chown_uid=999, lorax_args={})
        lorax_cmd = (
            'lorax '
            '--logfile=/tmp/nonexistentdirectory/tasks/123/123/logs/lorax.log '
            '/tmp/nonexistentdirectory/tasks/123/123/results; '
            'ret=$?; '
            'chmod -R a+r /tmp/nonexistentdirectory/tasks/123/123 '
            '&& chown -R 999 /tmp/nonexistentdirectory/tasks/123/123; '
            'exit $ret')
        self.session.host.subtask.assert_called_once_with(
            arglist=['tag_name', 'noarch', lorax_cmd],
            kwargs={'mounts': ['/mnt/koji'], 'packages': ['lorax']}, method='runroot', parent=123)

    @raises(koji.GenericError)
    def test_handler_outputdir_exists(self):
        self.t.handler('tag_name', 'noarch', packages=[], mounts=["/mnt/koji"], weight=10.0,
                       lorax_args={"outputdir": "/tmp"})
