# coding=utf-8
from __future__ import absolute_import
from __future__ import division

import logging
import optparse
import os
import random
import re
import socket
import string
import sys
import time
from contextlib import closing

import requests
import six
from six.moves import range

try:
    import krbV
except ImportError:  # pragma: no cover
    krbV = None

import koji
import koji.plugin

# fix OptionParser for python 2.3 (optparse verion 1.4.1+)
# code taken from optparse version 1.5a2
OptionParser = optparse.OptionParser
if optparse.__version__ == "1.4.1+":  # pragma: no cover
    def _op_error(self, msg):
        self.print_usage(sys.stderr)
        msg = "%s: error: %s\n" % (self._get_prog_name(), msg)
        if msg:
            sys.stderr.write(msg)
        sys.exit(2)
    OptionParser.error = _op_error

greetings = ('hello', 'hi', 'yo', "what's up", "g'day", 'back to work',
             'bonjour',
             'hallo',
             'ciao',
             'hola',
            u'olá',
            u'dobrý den',
            u'zdravstvuite',
            u'góðan daginn',
             'hej',
             'tervehdys',
            u'grüezi',
            u'céad míle fáilte',
            u'hylô',
            u'bună ziua',
            u'jó napot',
             'dobre dan',
            u'你好',
            u'こんにちは',
            u'नमस्कार',
            u'안녕하세요')

ARGMAP = {'None': None,
          'True': True,
          'False': False}


def _(args):
    """Stub function for translation"""
    return args


def arg_filter(arg):
    try:
        return int(arg)
    except ValueError:
        pass
    try:
        return float(arg)
    except ValueError:
        pass
    if arg in ARGMAP:
        return ARGMAP[arg]
    #handle lists/dicts?
    return arg


categories = {
    'admin' : 'admin commands',
    'build' : 'build commands',
    'search' : 'search commands',
    'download' : 'download commands',
    'monitor'  : 'monitor commands',
    'info' : 'info commands',
    'bind' : 'bind commands',
    'misc' : 'miscellaneous commands',
}


def get_epilog_str(progname=None):
    if progname is None:
        progname = os.path.basename(sys.argv[0]) or 'koji'
    categories_ordered=', '.join(sorted(['all'] + list(categories.keys())))
    epilog_str = '''
Try "%(progname)s --help" for help about global options
Try "%(progname)s help" to get all available commands
Try "%(progname)s <command> --help" for help about the options of a particular command
Try "%(progname)s help <category>" to get commands under a particular category
Available categories are: %(categories)s
''' % ({'progname': progname, 'categories': categories_ordered})
    return _(epilog_str)


def ensure_connection(session):
    try:
        ret = session.getAPIVersion()
    except six.moves.xmlrpc_client.ProtocolError:
        error(_("Error: Unable to connect to server"))
    if ret != koji.API_VERSION:
        warn(_("WARNING: The server is at API version %d and the client is at %d" % (ret, koji.API_VERSION)))


def print_task_headers():
    """Print the column headers"""
    print("ID       Pri  Owner                State    Arch       Name")


def print_task(task,depth=0):
    """Print a task"""
    task = task.copy()
    task['state'] = koji.TASK_STATES.get(task['state'],'BADSTATE')
    fmt = "%(id)-8s %(priority)-4s %(owner_name)-20s %(state)-8s %(arch)-10s "
    if depth:
        indent = "  "*(depth-1) + " +"
    else:
        indent = ''
    label = koji.taskLabel(task)
    print(''.join([fmt % task, indent, label]))


def print_task_recurse(task,depth=0):
    """Print a task and its children"""
    print_task(task,depth)
    for child in task.get('children',()):
        print_task_recurse(child,depth+1)


def parse_arches(arches, to_list=False):
    """Parse comma or space-separated list of arches and return
       only space-separated one."""
    arches = arches.replace(',', ' ').split()
    if to_list:
        return arches
    else:
        return ' '.join(arches)


class TaskWatcher(object):

    def __init__(self,task_id,session,level=0,quiet=False):
        self.id = task_id
        self.session = session
        self.info = None
        self.level = level
        self.quiet = quiet

    #XXX - a bunch of this stuff needs to adapt to different tasks

    def str(self):
        if self.info:
            label = koji.taskLabel(self.info)
            return "%s%d %s" % ('  ' * self.level, self.id, label)
        else:
            return "%s%d" % ('  ' * self.level, self.id)

    def __str__(self):
        return self.str()

    def get_failure(self):
        """Print information about task completion"""
        if self.info['state'] != koji.TASK_STATES['FAILED']:
            return ''
        error = None
        try:
            self.session.getTaskResult(self.id)
        except (six.moves.xmlrpc_client.Fault,koji.GenericError) as e:
            error = e
        if error is None:
            # print("%s: complete" % self.str())
            # We already reported this task as complete in update()
            return ''
        else:
            return '%s: %s' % (error.__class__.__name__, str(error).strip())

    def update(self):
        """Update info and log if needed.  Returns True on state change."""
        if self.is_done():
            # Already done, nothing else to report
            return False
        last = self.info
        self.info = self.session.getTaskInfo(self.id, request=True)
        if self.info is None:
            if not self.quiet:
                print("No such task id: %i" % self.id)
            sys.exit(1)
        state = self.info['state']
        if last:
            #compare and note status changes
            laststate = last['state']
            if laststate != state:
                if not self.quiet:
                    print("%s: %s -> %s" % (self.str(), self.display_state(last), self.display_state(self.info)))
                return True
            return False
        else:
            # First time we're seeing this task, so just show the current state
            if not self.quiet:
                print("%s: %s" % (self.str(), self.display_state(self.info)))
            return False

    def is_done(self):
        if self.info is None:
            return False
        state = koji.TASK_STATES[self.info['state']]
        return (state in ['CLOSED','CANCELED','FAILED'])

    def is_success(self):
        if self.info is None:
            return False
        state = koji.TASK_STATES[self.info['state']]
        return (state == 'CLOSED')

    def display_state(self, info):
        # We can sometimes be passed a task that is not yet open, but
        # not finished either.  info would be none.
        if not info:
            return 'unknown'
        if info['state'] == koji.TASK_STATES['OPEN']:
            if info['host_id']:
                host = self.session.getHost(info['host_id'])
                return 'open (%s)' % host['name']
            else:
                return 'open'
        elif info['state'] == koji.TASK_STATES['FAILED']:
            return 'FAILED: %s' % self.get_failure()
        else:
            return koji.TASK_STATES[info['state']].lower()


def display_tasklist_status(tasks):
    free = 0
    open = 0
    failed = 0
    done = 0
    for task_id in tasks.keys():
        status = tasks[task_id].info['state']
        if status == koji.TASK_STATES['FAILED']:
            failed += 1
        elif status == koji.TASK_STATES['CLOSED'] or status == koji.TASK_STATES['CANCELED']:
            done += 1
        elif status == koji.TASK_STATES['OPEN'] or status == koji.TASK_STATES['ASSIGNED']:
            open += 1
        elif status == koji.TASK_STATES['FREE']:
            free += 1
    print("  %d free  %d open  %d done  %d failed" % (free, open, done, failed))


def display_task_results(tasks):
    for task in [task for task in tasks.values() if task.level == 0]:
        state = task.info['state']
        task_label = task.str()

        if state == koji.TASK_STATES['CLOSED']:
            print('%s completed successfully' % task_label)
        elif state == koji.TASK_STATES['FAILED']:
            print('%s failed' % task_label)
        elif state == koji.TASK_STATES['CANCELED']:
            print('%s was canceled' % task_label)
        else:
            # shouldn't happen
            print('%s has not completed' % task_label)


def watch_tasks(session, tasklist, quiet=False, poll_interval=60):
    if not tasklist:
        return
    if not quiet:
        print("Watching tasks (this may be safely interrupted)...")
    sys.stdout.flush()
    rv = 0
    try:
        tasks = {}
        for task_id in tasklist:
            tasks[task_id] = TaskWatcher(task_id, session, quiet=quiet)
        while True:
            all_done = True
            for task_id, task in list(tasks.items()):
                changed = task.update()
                if not task.is_done():
                    all_done = False
                else:
                    if changed:
                        # task is done and state just changed
                        if not quiet:
                            display_tasklist_status(tasks)
                    if task.level == 0 and not task.is_success():
                        rv = 1
                for child in session.getTaskChildren(task_id):
                    child_id = child['id']
                    if not child_id in list(tasks.keys()):
                        tasks[child_id] = TaskWatcher(child_id, session, task.level + 1, quiet=quiet)
                        tasks[child_id].update()
                        # If we found new children, go through the list again,
                        # in case they have children also
                        all_done = False
            if all_done:
                if not quiet:
                    print('')
                    display_task_results(tasks)
                break

            sys.stdout.flush()
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        if tasks and not quiet:
            progname = os.path.basename(sys.argv[0]) or 'koji'
            tlist = ['%s: %s' % (t.str(), t.display_state(t.info))
                            for t in tasks.values() if not t.is_done()]
            print( \
"""Tasks still running. You can continue to watch with the '%s watch-task' command.
Running Tasks:
%s""" % (progname, '\n'.join(tlist)))
        raise
    return rv


def watch_logs(session, tasklist, opts, poll_interval):
    print("Watching logs (this may be safely interrupted)...")

    def _isDone(session, taskId):
        info = session.getTaskInfo(taskId)
        if info is None:
            print("No such task id: %i" % taskId)
            sys.exit(1)
        state = koji.TASK_STATES[info['state']]
        return (state in ['CLOSED','CANCELED','FAILED'])

    offsets = {}
    for task_id in tasklist:
        offsets[task_id] = {}

    lastlog = None
    while True:
        for task_id in tasklist[:]:
            if _isDone(session, task_id):
                tasklist.remove(task_id)

            output = list_task_output_all_volumes(session, task_id)
            # convert to list of (file, volume)
            files = []
            for filename, volumes in six.iteritems(output):
                files += [(filename, volume) for volume in volumes]

            if opts.log:
                logs = [file_volume for file_volume in files if file_volume[0] == opts.log]
            else:
                logs = [file_volume for file_volume in files if file_volume[0].endswith('log')]

            taskoffsets = offsets[task_id]
            for log, volume in logs:
                contents = 'placeholder'
                while contents:
                    if (log, volume) not in taskoffsets:
                        taskoffsets[(log, volume)] = 0

                    contents = session.downloadTaskOutput(task_id, log, taskoffsets[(log, volume)], 16384, volume=volume)
                    taskoffsets[(log, volume)] += len(contents)
                    if contents:
                        currlog = "%d:%s:%s:" % (task_id, volume, log)
                        if currlog != lastlog:
                            if lastlog:
                                sys.stdout.write("\n")
                            sys.stdout.write("==> %s <==\n" % currlog)
                            lastlog = currlog
                        sys.stdout.write(contents.decode('utf8'))

            if opts.follow:
                for child in session.getTaskChildren(task_id):
                    if child['id'] not in tasklist:
                        tasklist.append(child['id'])
                        offsets[child['id']] = {}

        if not tasklist:
            break

        time.sleep(poll_interval)


def list_task_output_all_volumes(session, task_id):
    """List task output with all volumes, or fake it"""
    try:
        return session.listTaskOutput(task_id, all_volumes=True)
    except koji.GenericError as e:
        if 'got an unexpected keyword argument' not in str(e):
            raise
    # otherwise leave off the option and fake it
    output = session.listTaskOutput(task_id)
    return dict([fn, ['DEFAULT']] for fn in output)


def _unique_path(prefix):
    """Create a unique path fragment by appending a path component
    to prefix.  The path component will consist of a string of letter and numbers
    that is unlikely to be a duplicate, but is not guaranteed to be unique."""
    # Use time() in the dirname to provide a little more information when
    # browsing the filesystem.
    # For some reason repr(time.time()) includes 4 or 5
    # more digits of precision than str(time.time())
    return '%s/%r.%s' % (prefix, time.time(),
                      ''.join([random.choice(string.ascii_letters) for i in range(8)]))


def _format_size(size):
    if (size / 1073741824 >= 1):
        return "%0.2f GiB" % (size / 1073741824.0)
    if (size / 1048576 >= 1):
        return "%0.2f MiB" % (size / 1048576.0)
    if (size / 1024 >=1):
        return "%0.2f KiB" % (size / 1024.0)
    return "%0.2f B" % (size)


def _format_secs(t):
    h = t / 3600
    t %= 3600
    m = t / 60
    s = t % 60
    return "%02d:%02d:%02d" % (h, m, s)


def _progress_callback(uploaded, total, piece, time, total_time):
    if total == 0:
        percent_done = 0.0
    else:
        percent_done = float(uploaded)/float(total)
    percent_done_str = "%02d%%" % (percent_done * 100)
    data_done = _format_size(uploaded)
    elapsed = _format_secs(total_time)

    speed = "- B/sec"
    if (time):
        if (uploaded != total):
            speed = _format_size(float(piece)/float(time)) + "/sec"
        else:
            speed = _format_size(float(total)/float(total_time)) + "/sec"

    # write formated string and flush
    sys.stdout.write("[% -36s] % 4s % 8s % 10s % 14s\r" % ('='*(int(percent_done*36)), percent_done_str, elapsed, data_done, speed))
    sys.stdout.flush()


def _running_in_bg():
    try:
        return (not os.isatty(0)) or (os.getpgrp() != os.tcgetpgrp(0))
    except OSError:
        return True


def linked_upload(localfile, path, name=None):
    """Link a file into the (locally writable) workdir, bypassing upload"""
    old_umask = os.umask(0o02)
    try:
        if name is None:
            name = os.path.basename(localfile)
        dest_dir = os.path.join(koji.pathinfo.work(), path)
        dst = os.path.join(dest_dir, name)
        koji.ensuredir(dest_dir)
        # fix uid/gid to keep httpd happy
        st = os.stat(koji.pathinfo.work())
        os.chown(dest_dir, st.st_uid, st.st_gid)
        print("Linking rpm to: %s" % dst)
        os.link(localfile, dst)
    finally:
        os.umask(old_umask)


def download_file(url, relpath, quiet=False, noprogress=False, size=None, num=None):
    """Download files from remote"""

    if '/' in relpath:
        koji.ensuredir(os.path.dirname(relpath))
    if not quiet:
        if size and num:
            print(_("Downloading [%d/%d]: %s") % (num, size, relpath))
        else:
            print(_("Downloading: %s") % relpath)


    with closing(requests.get(url, stream=True)) as response:
        length = response.headers.get('content-length')
        f = open(relpath, 'wb')
        if length is None:
            f.write(response.content)
            length = len(response.content)
            if not (quiet or noprogress):
                _download_progress(length, length)
        else:
            l = 0
            length = int(length)
            for chunk in response.iter_content(chunk_size=65536):
                l += len(chunk)
                f.write(chunk)
                if not (quiet or noprogress):
                    _download_progress(length, l)
            f.close()

    if not (quiet or noprogress):
        print('')


def _download_progress(download_t, download_d):
    if download_t == 0:
        percent_done = 0.0
    else:
        percent_done = float(download_d) / float(download_t)
    percent_done_str = "%3d%%" % (percent_done * 100)
    data_done = _format_size(download_d)

    sys.stdout.write("[% -36s] % 4s % 10s\r" % ('=' * (int(percent_done * 36)), percent_done_str, data_done))
    sys.stdout.flush()


def error(msg=None, code=1):
    if msg:
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
    sys.exit(code)


def warn(msg):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def has_krb_creds():
    if krbV is None:
        return False
    try:
        ctx = krbV.default_context()
        ccache = ctx.default_ccache()
        ccache.principal()
        return True
    except krbV.Krb5Error:
        return False


def activate_session(session, options):
    """Test and login the session is applicable"""
    if isinstance(options, dict):
        options = optparse.Values(options)
    noauth = options.authtype == "noauth" or getattr(options, 'noauth', False)
    runas = getattr(options, 'runas', None)
    if noauth:
        #skip authentication
        pass
    elif options.authtype == "ssl" or os.path.isfile(options.cert) and options.authtype is None:
        # authenticate using SSL client cert
        session.ssl_login(options.cert, None, options.serverca, proxyuser=runas)
    elif options.authtype == "password" or getattr(options, 'user', None) and options.authtype is None:
        # authenticate using user/password
        session.login()
    elif options.authtype == "kerberos" or has_krb_creds() and options.authtype is None:
        try:
            if options.keytab and options.principal:
                session.krb_login(principal=options.principal, keytab=options.keytab, proxyuser=runas)
            else:
                session.krb_login(proxyuser=runas)
        except socket.error as e:
            warn(_("Could not connect to Kerberos authentication service: %s") % e.args[1])
        except Exception as e:
            if krbV is not None and isinstance(e, krbV.Krb5Error):
                error(_("Kerberos authentication failed: %s (%s)") % (e.args[1], e.args[0]))
            else:
                raise
    if not noauth and not session.logged_in:
        error(_("Unable to log in, no authentication methods available"))
    ensure_connection(session)
    if options.debug:
        print("successfully connected to hub")


def _list_tasks(options, session):
    "Retrieve a list of tasks"

    callopts = {
        'state' : [koji.TASK_STATES[s] for s in ('FREE', 'OPEN', 'ASSIGNED')],
        'decode' : True,
    }

    if getattr(options, 'mine', False):
        if getattr(options, 'user', None):
            raise koji.GenericError("Can't specify 'mine' and 'user' in same time")
        user = session.getLoggedInUser()
        if not user:
            print("Unable to determine user")
            sys.exit(1)
        callopts['owner'] = user['id']
    if getattr(options, 'user', None):
        user = session.getUser(options.user)
        if not user:
            print("No such user: %s" % options.user)
            sys.exit(1)
        callopts['owner'] = user['id']
    if getattr(options, 'arch', None):
        callopts['arch'] = parse_arches(options.arch, to_list=True)
    if getattr(options, 'method', None):
        callopts['method'] = options.method
    if getattr(options, 'channel', None):
        chan = session.getChannel(options.channel)
        if not chan:
            print("No such channel: %s" % options.channel)
            sys.exit(1)
        callopts['channel_id'] = chan['id']
    if getattr(options, 'host', None):
        host = session.getHost(options.host)
        if not host:
            print("No such host: %s" % options.host)
            sys.exit(1)
        callopts['host_id'] = host['id']

    qopts = {'order' : 'priority,create_time'}
    tasklist = session.listTasks(callopts, qopts)
    tasks = dict([(x['id'], x) for x in tasklist])

    #thread the tasks
    for t in tasklist:
        if t['parent'] is not None:
            parent = tasks.get(t['parent'])
            if parent:
                parent.setdefault('children',[])
                parent['children'].append(t)
                t['sub'] = True

    return tasklist


def register_plugin(plugin):
    """Scan a given plugin for handlers

    Handlers are functions marked with one of the decorators defined in koji.plugin
    """
    for v in six.itervalues(vars(plugin)):
        if isinstance(v, six.class_types):
            #skip classes
            continue
        if callable(v):
            if getattr(v, 'exported_cli', False):
                if hasattr(v, 'export_alias'):
                    name = getattr(v, 'export_alias')
                else:
                    name = v.__name__
                # copy object to local namespace
                setattr(CommandExports, name, staticmethod(v))


def load_plugins(options, paths):
    """Load plugins specified by our configuration plus system plugins. Order
    is that system plugins are first, so they can be overridden by
    user-specified ones with same name."""
    logger = logging.getLogger('koji.plugins')
    tracker = koji.plugin.PluginTracker(path=paths)
    names = set()
    for path in paths:
        if os.path.exists(path):
            for name in sorted(os.listdir(path)):
                if not name.endswith('.py'):
                    continue
                name = name[:-3]
                names.add(name)
    for name in names:
        logger.info('Loading plugin: %s', name)
        tracker.load(name)
        register_plugin(tracker.get(name))


def get_options(no_cmd=False):
    """process options from command line and config file.

    If no_cmd is False, command is required, and it must be registered.
    Returns (options, cmd, args)

    If no_cmd is True, command would be an option.
    Returns (options, args)
    It's usually used by standalone cli script,
    but global options and customized options should be divided by '--'.
    """

    common_commands = ['build', 'help', 'download-build',
                       'latest-pkg', 'search', 'list-targets']
    usage = _("%%prog [global-options] command [command-options-and-arguments]"
                "\n\nCommon commands: %s" % ', '.join(sorted(common_commands)))
    parser = OptionParser(usage=usage)
    parser.disable_interspersed_args()
    progname = os.path.basename(sys.argv[0]) or 'koji'
    parser.__dict__['origin_format_help'] = parser.format_help
    parser.__dict__['format_help'] = lambda formatter=None: (
        "%(origin_format_help)s%(epilog)s" % ({
            'origin_format_help': parser.origin_format_help(formatter),
            'epilog': get_epilog_str()}))
    parser.add_option("-c", "--config", dest="configFile",
                      help=_("use alternate configuration file"), metavar="FILE")
    parser.add_option("-p", "--profile", default=progname,
                      help=_("specify a configuration profile"))
    parser.add_option("--keytab", help=_("specify a Kerberos keytab to use"), metavar="FILE")
    parser.add_option("--principal", help=_("specify a Kerberos principal to use"))
    parser.add_option("--krbservice", help=_("specify the Kerberos service name for the hub"))
    parser.add_option("--runas", help=_("run as the specified user (requires special privileges)"))
    parser.add_option("--user", help=_("specify user"))
    parser.add_option("--password", help=_("specify password"))
    parser.add_option("--noauth", action="store_true", default=False,
                      help=_("do not authenticate"))
    parser.add_option("--force-auth", action="store_true", default=False,
                      help=_("authenticate even for read-only operations"))
    parser.add_option("--authtype", help=_("force use of a type of authentication, options: noauth, ssl, password, or kerberos"))
    parser.add_option("-d", "--debug", action="store_true",
                      help=_("show debug output"))
    parser.add_option("--debug-xmlrpc", action="store_true",
                      help=_("show xmlrpc debug output"))
    parser.add_option("-q", "--quiet", action="store_true", default=False,
                      help=_("run quietly"))
    parser.add_option("--skip-main", action="store_true", default=False,
                      help=_("don't actually run main"))
    parser.add_option("-s", "--server", help=_("url of XMLRPC server"))
    parser.add_option("--topdir", help=_("specify topdir"))
    parser.add_option("--weburl", help=_("url of the Koji web interface"))
    parser.add_option("--topurl", help=_("url for Koji file access"))
    parser.add_option("--pkgurl", help=optparse.SUPPRESS_HELP)
    parser.add_option("--plugin-paths", help=_("specify plugin paths divided by ':'"))
    parser.add_option("--help-commands", action="store_true", default=False, help=_("list commands"))
    (options, args) = parser.parse_args()

    # load local config
    try:
        result = koji.read_config(options.profile, user_config=options.configFile)
    except koji.ConfigurationError as e:
        parser.error(e.args[0])
        assert False  # pragma: no cover

    # update options according to local config
    for name, value in six.iteritems(result):
        if getattr(options, name, None) is None:
            setattr(options, name, value)

    dir_opts = ('topdir', 'cert', 'serverca')
    for name in dir_opts:
        # expand paths here, so we don't have to worry about it later
        value = os.path.expanduser(getattr(options, name))
        setattr(options, name, value)

    #honor topdir
    if options.topdir:
        koji.BASEDIR = options.topdir
        koji.pathinfo.topdir = options.topdir

    #pkgurl is obsolete
    if options.pkgurl:
        if options.topurl:
            warn("Warning: the pkgurl option is obsolete")
        else:
            suggest = re.sub(r'/packages/?$', '', options.pkgurl)
            if suggest != options.pkgurl:
                warn("Warning: the pkgurl option is obsolete, using topurl=%r"
                     % suggest)
                options.topurl = suggest
            else:
                warn("Warning: The pkgurl option is obsolete, please use topurl instead")


    # update plugin_paths to list
    plugin_paths = options.plugin_paths or []
    if plugin_paths:
        plugin_paths = [os.path.expanduser(p) for p in plugin_paths.split(':')]
    # always load plugins from koji_cli_plugins module
    plugin_paths.append('%s/lib/python%s.%s/site-packages/koji_cli_plugins' %
                        (sys.prefix, sys.version_info[0], sys.version_info[1]))
    setattr(options, 'plugin_paths', plugin_paths)
    load_plugins(options, plugin_paths)

    if no_cmd:
        return options, args
    if options.help_commands:
        list_commands()
        sys.exit(0)
    if not args:
        list_commands()
        sys.exit(0)

    aliases = {
        'cancel-task' : 'cancel',
        'cxl' : 'cancel',
        'list-commands' : 'help',
        'move-pkg': 'move-build',
        'move': 'move-build',
        'latest-pkg': 'latest-build',
        'tag-pkg': 'tag-build',
        'tag': 'tag-build',
        'untag-pkg': 'untag-build',
        'untag': 'untag-build',
        'watch-tasks': 'watch-task',
    }
    cmd = args[0]
    cmd = aliases.get(cmd, cmd)
    if cmd.lower() in greetings:
        cmd = "moshimoshi"
    cmd = cmd.replace('-', '_')
    if hasattr(CommandExports, 'anon_handle_' + cmd):
        if not options.force_auth and '--mine' not in args:
            options.noauth = True
        cmd = 'anon_handle_' + cmd
    elif hasattr(CommandExports, 'handle_' + cmd):
        cmd = 'handle_' + cmd
    else:
        list_commands()
        parser.error('Unknown command: %s' % args[0])
        assert False  # pragma: no cover

    return options, cmd, args[1:]


def list_commands(categories_chosen=None):
    if categories_chosen is None or "all" in categories_chosen:
        categories_chosen = list(categories.keys())
    else:
        # copy list since we're about to modify it
        categories_chosen = list(categories_chosen)
    categories_chosen.sort()
    handlers = []
    for name, value in six.iteritems(vars(CommandExports)):
        if name.startswith('handle_'):
            alias = name.replace('handle_', '')
            alias = alias.replace('_', '-')
            handlers.append((alias, value))
        elif name.startswith('anon_handle_'):
            alias = name.replace('anon_handle_', '')
            alias = alias.replace('_', '-')
            handlers.append((alias, value))
    handlers.sort()
    print(_("Available commands:"))
    for category in categories_chosen:
        print(_("\n%s:" % categories[category]))
        for alias, handler in handlers:
            if isinstance(handler, staticmethod):
                handler = handler.__func__
            desc = handler.__doc__ or ''
            if desc.startswith('[%s] ' % category):
                desc = desc[len('[%s] ' % category):]
            elif category != 'misc' or desc.startswith('['):
                continue
            print("        %-25s %s" % (alias, desc))

    print("%s" % get_epilog_str().rstrip("\n"))


class CommandExports(object):

    @staticmethod
    def handle_help(options, session, args):
        "[info] List available commands"
        usage = _("usage: %prog help <category> ...")
        usage += _("\n(Specify the --help global option for a list of other help options)")
        parser = OptionParser(usage=usage)
        # the --admin opt is for backwards compatibility. It is equivalent to: koji help admin
        parser.add_option("--admin", action="store_true", help=optparse.SUPPRESS_HELP)

        (options, args) = parser.parse_args(args)

        chosen = set(args)
        if options.admin:
            chosen.add('admin')
        avail = set(list(categories.keys()) + ['all'])
        unavail = chosen - avail
        for arg in unavail:
            print("No such help category: %s" % arg)

        if not chosen:
            list_commands()
        else:
            list_commands(chosen)
