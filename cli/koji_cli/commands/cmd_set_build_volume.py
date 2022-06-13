from __future__ import absolute_import, division

from optparse import OptionParser



from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_set_build_volume(goptions, session, args):
    "[admin] Move a build to a different volume"
    usage = "usage: %prog set-build-volume <volume> <n-v-r> [<n-v-r> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-v", "--verbose", action="store_true", help="Be verbose")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("You must provide a volume and at least one build")
    volinfo = session.getVolume(args[0])
    if not volinfo:
        error("No such volume: %s" % args[0])
    activate_session(session, goptions)
    builds = []
    for nvr in args[1:]:
        binfo = session.getBuild(nvr)
        if not binfo:
            print("No such build: %s" % nvr)
        elif binfo['volume_id'] == volinfo['id']:
            print("Build %s already on volume %s" % (nvr, volinfo['name']))
        else:
            builds.append(binfo)
    if not builds:
        error("No builds to move")
    for binfo in builds:
        session.changeBuildVolume(binfo['id'], volinfo['id'])
        if options.verbose:
            print("%s: %s -> %s" % (binfo['nvr'], binfo['volume_name'], volinfo['name']))


