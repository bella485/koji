from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_list_api(goptions, session, args):
    "[info] Print the list of XML-RPC APIs"
    usage = "usage: %prog list-api [options] [method_name ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    ensure_connection(session, goptions)
    if args:
        for method in args:
            help = session.system.methodHelp(method)
            if not help:
                parser.error("Unknown method: %s" % method)
            print(help)
    else:
        for x in sorted(session._listapi(), key=lambda x: x['name']):
            if 'argdesc' in x:
                args = x['argdesc']
            elif x['args']:
                # older servers may not provide argdesc
                expanded = []
                for arg in x['args']:
                    if isinstance(arg, str):
                        expanded.append(arg)
                    else:
                        expanded.append('%s=%s' % (arg[0], arg[1]))
                args = "(%s)" % ", ".join(expanded)
            else:
                args = "()"
            print('%s%s' % (x['name'], args))
            if x['doc']:
                print("  description: %s" % x['doc'])


