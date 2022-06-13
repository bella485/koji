from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str
)


def anon_handle_list_untagged(goptions, session, args):
    "[info] List untagged builds"
    usage = "usage: %prog list-untagged [options] [<package>]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--paths", action="store_true", help="Show the file paths")
    parser.add_option("--show-references", action="store_true", help="Show build references")
    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error("Only one package name may be specified")
    ensure_connection(session, goptions)
    package = None
    if len(args) > 0:
        package = args[0]
    opts = {}
    if package:
        package_id = session.getPackageID(package)
        if package_id is None:
            error("No such package: %s" % package)
        opts['name'] = package
    pathinfo = koji.PathInfo()

    data = session.untaggedBuilds(**opts)
    if options.show_references:
        print("(Showing build references)")
        references = {}
        with session.multicall(strict=True, batch=10000) as m:
            for build in data:
                references[build['id']] = m.buildReferences(build['id'])

        for build in data:
            refs = references[build['id']].result
            r = []
            if refs.get('rpms'):
                r.append("rpms: %s" % refs['rpms'])
            if refs.get('component_of'):
                r.append("images/archives: %s" % refs['component_of'])
            if refs.get('archives'):
                r.append("archives buildroots: %s" % refs['archives'])
            build['refs'] = ', '.join(r)
    if options.paths:
        for x in data:
            x['path'] = pathinfo.build(x)
        fmt = "%(path)s"
    else:
        fmt = "%(name)s-%(version)s-%(release)s"
    if options.show_references:
        fmt = fmt + "  %(refs)s"
    output = sorted([fmt % x for x in data])
    for line in output:
        print(line)
