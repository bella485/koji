#!/usr/bin/python3
import os
import shutil

HEAD = """from __future__ import absolute_import, division

import ast
import fnmatch
import itertools
import hashlib
import json
import logging
import os
import pprint
import random
import re
import stat
import sys
import textwrap
import time
import traceback
from collections import OrderedDict, defaultdict
from datetime import datetime
from dateutil.tz import tzutc
from optparse import SUPPRESS_HELP, OptionParser

import six
import six.moves.xmlrpc_client
from six.moves import filter, map, range, zip

import koji
from koji.util import base64encode, md5_constructor, to_list

from koji_cli.lib import (
    TimeOption,
    DatetimeJSONEncoder,
    _list_tasks,
    _progress_callback,
    _running_in_bg,
    activate_session,
    arg_filter,
    download_archive,
    download_file,
    download_rpm,
    ensure_connection,
    error,
    format_inheritance_flags,
    get_usage_str,
    greetings,
    linked_upload,
    list_task_output_all_volumes,
    print_task_headers,
    print_task_recurse,
    unique_path,
    warn,
    watch_logs,
    watch_tasks,
    truncate_string
)
"""

try:
    shutil.rmtree('commands')
except:
    pass
os.mkdir('commands')

init = open('commands/__init__.py', 'wt')
out = open('xyz.py', 'wt')
out_cmd = None

state = 'out'
for line in open('c.py'):
    if state == 'out':
        if line.startswith('def '):
            fname = line[4:].split('(')[0]
            if fname.startswith('handle_') or fname.startswith('anon_handle_'):
                if fname.startswith('handle_'):
                    fn = f'cmd_{fname[7:]}'
                else:
                    fn = f'cmd_{fname[12:]}'
                out_cmd = open(os.path.join('commands', "%s.py" % fn), 'wt')
                out_cmd.write(HEAD)
                out_cmd.write(line)
                init.write('from .%s import %s\n' % (fn, fname))
                state = 'in'
            else:
                out.write(line)
        else:
            out.write(line)
    elif state == 'in':
        if line.startswith('def '):
            fname = line[4:].split('(')[0]
            if fname.startswith('handle_') or fname.startswith('anon_handle_'):
                if fname.startswith('handle_'):
                    fn = f'cmd_{fname[7:]}'
                else:
                    fn = f'cmd_{fname[12:]}'
                out_cmd = open(os.path.join('commands', "%s.py" % fn), 'wt')
                out_cmd.write(HEAD)
                out_cmd.write(line)
                init.write('from .%s import %s\n' % (fn, fname))
                state = 'in'
            else:
                out_cmd.write(line)
        else:
            out_cmd.write(line)
