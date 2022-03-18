from __future__ import absolute_import

import mock
from mock import call
import six
from six.moves import StringIO
import koji
from koji_cli.commands import anon_handle_download_scratch_build
from . import utils


class TestDownloadScratchBuild(utils.CliTestCase):
    # Show long diffs in error output...
    maxDiff = None

    def setUp(self):
        self.options = mock.MagicMock()
        self.options.quiet = False
        self.options.topurl = 'https://topurl'
        self.session = mock.MagicMock()
        self.session.getAPIVersion.return_value = koji.API_VERSION
        self.stdout = mock.patch('sys.stdout', new_callable=six.StringIO).start()
        self.ensure_connection = mock.patch('koji_cli.commands.ensure_connection').start()
        self.download_file = mock.patch('koji_cli.commands.download_file').start()
        self.list_task_output_all_volumes = mock.patch(
            'koji_cli.commands.list_task_output_all_volumes').start()


    def test_download_scratch_build(self):
        task_id = 123333
        args = [str(task_id)]
        self.session.getTaskInfo.return_value = {'id': task_id,
                                                 'parent': None,
                                                 'method': 'image'}
        self.session.getTaskChildren.return_value = [{'id': 22222,
                                                      'parent': 123333,
                                                      'method': 'createImage',
                                                      'arch': 'noarch',
                                                      'state': 2}]
        self.list_task_output_all_volumes.return_value = {
                                 'ks-name_koji-tag.ks': ['DEFAULT'],
                                 'oz-taskarch.log': ['DEFAULT'],
                                 'ks-name.ks': ['DEFAULT'],
                                 'tdl-taskarch.xml': ['DEFAULT'],
                                 'build-name_date_release.tar.xz': ['DEFAULT']}

        rv = anon_handle_download_scratch_build(self.options, self.session, args)
        actual = self.stdout.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)
        self.ensure_connection.assert_called_once_with(self.session, self.options)
        self.assertEqual(self.list_task_output_all_volumes.mock_calls, [call(self.session, 22222)])
        self.assertListEqual(self.download_file.mock_calls, [
            call('https://topurl/work/tasks/2222/22222/ks-name_koji-tag.ks',
                 'ks-name_koji-tag.ks', filesize=None, quiet=False, noprogress=None),
            call('https://topurl/work/tasks/2222/22222/ks-name.ks',
                 'ks-name.ks', filesize=None, quiet=False, noprogress=None),
            call('https://topurl/work/tasks/2222/22222/tdl-taskarch.xml',
                 'tdl-taskarch.xml', filesize=None, quiet=False, noprogress=None),
            call('https://topurl/work/tasks/2222/22222/build-name_date_release.tar.xz',
                 'build-name_date_release.tar.xz', filesize=None, quiet=False, noprogress=None)])
        self.assertIsNone(rv)


    def test_download_scratch_build_wlogs(self):
        task_id = 123333
        args = [str(task_id), "--logs"]
        self.session.getTaskInfo.return_value = {'id': task_id,
                                                 'parent': None,
                                                 'method': 'image'}
        self.session.getTaskChildren.return_value = [{'id': 22222,
                                                      'parent': 123333,
                                                      'method': 'createImage',
                                                      'arch': 'noarch',
                                                      'state': 2}]
        self.list_task_output_all_volumes.return_value = {
                                 'ks-name_koji-tag.ks': ['DEFAULT'],
                                 'oz-taskarch.log': ['DEFAULT'],
                                 'ks-name.ks': ['DEFAULT'],
                                 'tdl-taskarch.xml': ['DEFAULT'],
                                 'build-name_date_release.tar.xz': ['DEFAULT']}

        rv = anon_handle_download_scratch_build(self.options, self.session, args)
        actual = self.stdout.getvalue()
        expected = ''
        self.assertMultiLineEqual(actual, expected)
        self.ensure_connection.assert_called_once_with(self.session, self.options)
        self.assertEqual(self.list_task_output_all_volumes.mock_calls, [call(self.session, 22222)])
        self.assertListEqual(self.download_file.mock_calls, [
             call('https://topurl/work/tasks/2222/22222/ks-name_koji-tag.ks',
                  'ks-name_koji-tag.ks', filesize=None, quiet=False, noprogress=None),
             call('https://topurl/work/tasks/2222/22222/oz-taskarch.log',
                  'oz-taskarch.log', filesize=None, quiet=False, noprogress=None),
             call('https://topurl/work/tasks/2222/22222/ks-name.ks',
                  'ks-name.ks', filesize=None, quiet=False, noprogress=None),
             call('https://topurl/work/tasks/2222/22222/tdl-taskarch.xml',
                  'tdl-taskarch.xml', filesize=None, quiet=False, noprogress=None),
             call('https://topurl/work/tasks/2222/22222/build-name_date_release.tar.xz',
                  'build-name_date_release.tar.xz', filesize=None, quiet=False, noprogress=None)])
        self.assertIsNone(rv)


    @mock.patch('sys.stderr', new_callable=StringIO)
    def test_download_scratch_build_nonscratch_img_id(self, stderr):
        task_id = 123333
        args = [str(task_id)]
        self.session.getTaskInfo.return_value = {'id': task_id,
                                                 'parent': None,
                                                 'method': 'image'}
        self.session.getTaskChildren.return_value = [{'id': 22222,
                                                      'parent': 123333,
                                                      'method': 'createImage',
                                                      'arch': 'noarch',
                                                      'state': 2},
                                                      {'id': 33333,
                                                      'parent': 123333,
                                                      'method': 'createImage',
                                                      'arch': 'noarch',
                                                      'state': 2}]
        expected = "No such scratch image build\n"
        with self.assertRaises(SystemExit) as ex:
            anon_handle_download_scratch_build(self.options, self.session, args)
        self.assertExitCode(ex, 1)
        self.assert_console_message(stderr, expected)


    @mock.patch('sys.stderr', new_callable=StringIO)
    def test_download_scratch_build_rpm_id(self, stderr):
        task_id = 123333
        args = [str(task_id)]
        self.session.getTaskInfo.return_value = {'id': task_id,
                                                 'parent': None,
                                                 'method': 'build'}
        self.session.getTaskChildren.return_value = [{'id': 22222,
                                                      'parent': 123333,
                                                      'method': 'rebuildSRPM',
                                                      'arch': 'noarch',
                                                      'state': 2},
                                                      {'id': 33333,
                                                      'parent': 123333,
                                                      'method': 'rebuildSRPM',
                                                      'arch': 'noarch',
                                                      'state': 2}]
        expected = "No such scratch image build\n"
        with self.assertRaises(SystemExit) as ex:
            anon_handle_download_scratch_build(self.options, self.session, args)
        self.assertExitCode(ex, 1)
        self.assert_console_message(stderr, expected)


    @mock.patch('sys.stderr', new_callable=StringIO)
    def test_download_scratch_build_without_option(self, stderr):
        expected = "Usage: %s download-scratch-build [options] <task-id>\n" \
                   "(Specify the --help global option for a list of other help options)\n\n" \
                   "%s: error: Please specify a valid task id\n" \
                   % (self.progname, self.progname)
        with self.assertRaises(SystemExit) as ex:
            anon_handle_download_scratch_build(self.options, self.session, [])
        self.assertExitCode(ex, 2)
        self.assert_console_message(stderr, expected)


    @mock.patch('sys.stderr', new_callable=StringIO)
    def test_download_scratch_build_invalid_option(self, stderr):
        invalid_option = "random_string_arg"
        expected = "The argument '%s' cannot be a task-id number\n" % invalid_option
        with self.assertRaises(SystemExit) as ex:
            anon_handle_download_scratch_build(self.options, self.session, [invalid_option])
        self.assertExitCode(ex, 1)
        self.assert_console_message(stderr, expected)


    @mock.patch('sys.stderr', new_callable=StringIO)
    def test_download_scratch_build_no_files(self, stderr):
        task_id = '1'
        expected = "No files were found for taskid %s\n" % task_id
        self.session.getTaskInfo.return_value = {'id': task_id,
                                                 'parent': None,
                                                 'method': 'image'}
        self.session.getTaskChildren.return_value = [{'id': 1,
                                                      'parent': 123333,
                                                      'method': 'createImage',
                                                      'arch': 'noarch',
                                                      'state': 2}]
        self.list_task_output_all_volumes.return_value = {}
        with self.assertRaises(SystemExit) as ex:
            anon_handle_download_scratch_build(self.options, self.session, [task_id])
        self.assertExitCode(ex, 1)
        self.assert_console_message(stderr, expected)


    def test_handle_add_volume_help(self):
        self.assert_help(
            anon_handle_download_scratch_build,
            """Usage: %s download-scratch-build [options] <task-id>
(Specify the --help global option for a list of other help options)

Options:
  -h, --help    show this help message and exit
  --logs        Also download build logs
  --topurl=URL  URL under which Koji files are accessible
  --noprogress  Do not display progress meter
  -q, --quiet   Suppress output
""" % self.progname)
