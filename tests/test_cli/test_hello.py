# coding=utf-8
import io
import mock
import unittest

import koji
from koji_cli.commands import handle_moshimoshi
from . import utils


class TestHello(utils.CliTestCase):

    # Show long diffs in error output...
    maxDiff = None

    def setUp(self):
        self.huburl = "https://%s.local/%shub" % (self.progname, self.progname)

    @mock.patch('sys.stdout', new_callable=io.StringIO)
    @mock.patch('sys.stderr', new_callable=io.StringIO)
    @mock.patch('koji_cli.commands.activate_session')
    def test_handle_moshimoshi(
            self,
            activate_session_mock,
            stderr,
            stdout):
        """Test handle_moshimoshi function"""
        user = {'name': self.progname,
                'krb_principal': '%s@localhost' % self.progname}
        cert = '/etc/pki/user.cert'
        options = mock.MagicMock()
        # Mock out the xmlrpc server
        session = mock.MagicMock(baseurl=self.huburl, authtype=None)
        session.getLoggedInUser.return_value = None
        session.krb_principal = user['krb_principal']

        expect = """Usage: %s moshimoshi [options]
(Specify the --help global option for a list of other help options)

%s: error: This command takes no arguments
""" % (self.progname, self.progname)

        self.assert_system_exit(
            handle_moshimoshi,
            options,
            session,
            ['argument'],
            stderr=expect,
            activate_session=None)

        auth_tests = {
            koji.AUTHTYPE_NORMAL: 'Authenticated via password',
            koji.AUTHTYPE_GSSAPI: 'Authenticated via GSSAPI',
            koji.AUTHTYPE_KERB: 'Authenticated via Kerberos principal %s' %
                                user['krb_principal'],
            koji.AUTHTYPE_SSL: 'Authenticated via client certificate %s' %
                                cert
        }

        message = "Not authenticated\n.+, anonymous user!"
        hubinfo = "You are using the hub at %s" % self.huburl
        handle_moshimoshi(options, session, [])
        self.assert_console_message(stdout, "{0}\n\n{1}\n".format(message, hubinfo),
                                    regex=True)

        session.getLoggedInUser.return_value = user
        message = ".+, %s!" % self.progname
        options.cert = cert
        for authtype, authinfo in auth_tests.items():
            session.authtype = authtype
            #print_unicode_mock.reset_mock()
            #print_unicode_mock.return_value = "Hello"
            handle_moshimoshi(options, session, [])
            #print_unicode_mock.assert_called_once()
            self.assert_console_message(
                stdout, "{0}\n\n{1}\n{2}\n".format(message, hubinfo, authinfo), regex=True)

    def test_handle_moshimoshi_help(self):
        self.assert_help(
            handle_moshimoshi,
            """Usage: %s moshimoshi [options]
(Specify the --help global option for a list of other help options)

Options:
  -h, --help  show this help message and exit
""" % self.progname)


if __name__ == '__main__':
    unittest.main()
