from __future__ import absolute_import, division

import os
import textwrap
from optparse import OptionParser


import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str
)


def anon_handle_latest_build(goptions, session, args):
    """[info] Print the latest builds for a tag"""
    usage = """\
        usage: %prog latest-build [options] <tag> <package> [<package> ...]

        The first option should be the name of a tag, not the name of a build target.
        If you want to know the latest build in buildroots for a given build target,
        then you should use the name of the build tag for that target. You can find
        this value by running '%prog list-targets --name=<target>'

        More information on tags and build targets can be found in the documentation.
        https://docs.pagure.org/koji/HOWTO/#package-organization"""

    usage = textwrap.dedent(usage)
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--arch", help="List all of the latest packages for this arch")
    parser.add_option("--all", action="store_true",
                      help="List all of the latest packages for this tag")
    parser.add_option("--quiet", action="store_true", default=goptions.quiet,
                      help="Do not print the header information")
    parser.add_option("--paths", action="store_true", help="Show the file paths")
    parser.add_option("--type",
                      help="Show builds of the given type only. "
                           "Currently supported types: maven, win, image, or any custom "
                           "content generator btypes")
    (options, args) = parser.parse_args(args)
    if len(args) == 0:
        parser.error("A tag name must be specified")
    ensure_connection(session, goptions)
    if options.all:
        if len(args) > 1:
            parser.error("A package name may not be combined with --all")
        # Set None as the package argument
        args.append(None)
    else:
        if len(args) < 2:
            parser.error("A tag name and package name must be specified")
    pathinfo = koji.PathInfo()

    for pkg in args[1:]:
        if options.arch:
            rpms, builds = session.getLatestRPMS(args[0], package=pkg, arch=options.arch)
            builds_hash = dict([(x['build_id'], x) for x in builds])
            data = rpms
            if options.paths:
                for x in data:
                    z = x.copy()
                    x['name'] = builds_hash[x['build_id']]['package_name']
                    x['path'] = os.path.join(pathinfo.build(x), pathinfo.rpm(z))
                fmt = "%(path)s"
            else:
                fmt = "%(name)s-%(version)s-%(release)s.%(arch)s"
        else:
            kwargs = {'package': pkg}
            if options.type:
                kwargs['type'] = options.type
            data = session.getLatestBuilds(args[0], **kwargs)
            if options.paths:
                if options.type == 'maven':
                    for x in data:
                        x['path'] = pathinfo.mavenbuild(x)
                    fmt = "%(path)-40s  %(tag_name)-20s  %(maven_group_id)-20s  " \
                          "%(maven_artifact_id)-20s  %(owner_name)s"
                else:
                    for x in data:
                        x['path'] = pathinfo.build(x)
                    fmt = "%(path)-40s  %(tag_name)-20s  %(owner_name)s"
            else:
                if options.type == 'maven':
                    fmt = "%(nvr)-40s  %(tag_name)-20s  %(maven_group_id)-20s  " \
                          "%(maven_artifact_id)-20s  %(owner_name)s"
                else:
                    fmt = "%(nvr)-40s  %(tag_name)-20s  %(owner_name)s"
            if not options.quiet:
                if options.type == 'maven':
                    print("%-40s  %-20s  %-20s  %-20s  %s" %
                          ("Build", "Tag", "Group Id", "Artifact Id", "Built by"))
                    print("%s  %s  %s  %s  %s" %
                          ("-" * 40, "-" * 20, "-" * 20, "-" * 20, "-" * 16))
                else:
                    print("%-40s  %-20s  %s" % ("Build", "Tag", "Built by"))
                    print("%s  %s  %s" % ("-" * 40, "-" * 20, "-" * 16))
                options.quiet = True

        output = sorted([fmt % x for x in data])
        for line in output:
            print(line)


