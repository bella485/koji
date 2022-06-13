from __future__ import absolute_import, division

import ast
import json
import pprint
import textwrap
from optparse import OptionParser



from koji_cli.lib import (
    DatetimeJSONEncoder,
    activate_session,
    arg_filter,
    get_usage_str
)


def handle_call(goptions, session, args):
    "Execute an arbitrary XML-RPC call"
    usage = """\
        usage: %prog call [options] <name> [<arg> ...]

        Note, that you can use global option --noauth for anonymous calls here"""
    usage = textwrap.dedent(usage)
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--python", action="store_true", help="Use python syntax for values")
    parser.add_option("--kwargs",
                      help="Specify keyword arguments as a dictionary (implies --python)")
    parser.add_option("--json-output", action="store_true", help="Use JSON syntax for output")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify the name of the XML-RPC method")
    if options.kwargs:
        options.python = True
    if options.python and ast is None:
        parser.error("The ast module is required to read python syntax")
    if options.json_output and json is None:
        parser.error("The json module is required to output JSON syntax")
    activate_session(session, goptions)
    name = args[0]
    non_kw = []
    kw = {}
    if options.python:
        non_kw = [ast.literal_eval(a) for a in args[1:]]
        if options.kwargs:
            kw = ast.literal_eval(options.kwargs)
    else:
        for arg in args[1:]:
            if arg.find('=') != -1:
                key, value = arg.split('=', 1)
                kw[key] = arg_filter(value)
            else:
                non_kw.append(arg_filter(arg))
    response = getattr(session, name).__call__(*non_kw, **kw)
    if options.json_output:
        print(json.dumps(response, indent=2, separators=(',', ': '), cls=DatetimeJSONEncoder))
    else:
        pprint.pprint(response)


