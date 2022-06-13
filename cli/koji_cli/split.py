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

INIT = """import importlib
import inspect
import pkgutil
import six

__all__ = []

for loader, name, is_pkg in pkgutil.walk_packages(__path__):
    if six.PY2:
        module = loader.find_module(name).load_module(name)
    else:
        spec = loader.find_spec(name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    for name, entry in inspect.getmembers(module):
        if name.startswith('handle_') or name.startswith('anon_handle_'):
            globals()[name] = entry
            __all__.append(name)
"""

try:
    shutil.rmtree('commands')
except:
    pass
os.mkdir('commands')

with open('commands/__init__.py', 'wt') as f:
    f.write(INIT)

out = open('xyz.py', 'wt')
out_cmd = None

state = 'out'
for line in open('c.py'):
    if state == 'out':
        if line.startswith('def '):
            fname = line[4:].split('(')[0]
            if fname.startswith('handle_') or fname.startswith('anon_handle_'):
                if fname.startswith('handle_'):
                    fn = f'{fname[7:]}'
                else:
                    fn = f'{fname[12:]}'
                out_cmd = open(os.path.join('commands', "%s.py" % fn), 'wt')
                out_cmd.write(HEAD)
                out_cmd.write(line)
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
                    fn = f'{fname[7:]}'
                else:
                    fn = f'{fname[12:]}'
                out_cmd = open(os.path.join('commands', "%s.py" % fn), 'wt')
                out_cmd.write(HEAD)
                out_cmd.write(line)
                state = 'in'
            else:
                out_cmd.write(line)
        else:
            out_cmd.write(line)

# run autoflake -i -r --remove-all-unused-imports to clean up headers
