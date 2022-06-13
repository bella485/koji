from __future__ import absolute_import, division

from optparse import OptionParser


import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_remove_sig(goptions, session, args):
    "[admin] Remove signed RPMs from db and disk"
    usage = "usage: %prog remove-sig [options] <rpm-id/n-v-r.a/rpminfo>"
    description = "Only use this method in extreme situations, because it "
    description += "goes against Koji's design of immutable, auditable data."
    parser = OptionParser(usage=get_usage_str(usage), description=description)
    parser.add_option("--sigkey", action="store", default=None, help="Specify signature key")
    parser.add_option("--all", action="store_true", default=False,
                      help="Remove all signed copies for specified RPM")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify an RPM")

    if not options.all and not options.sigkey:
        error("Either --sigkey or --all options must be given")

    if options.all and options.sigkey:
        error("Conflicting options specified")

    activate_session(session, goptions)
    rpminfo = args[0]

    try:
        session.deleteRPMSig(rpminfo, sigkey=options.sigkey, all_sigs=options.all)
    except koji.GenericError as e:
        msg = str(e)
        if msg.startswith("No such rpm"):
            # make this a little more readable than the hub error
            error("No such rpm in system: %s" % rpminfo)
        else:
            error("Signature removal failed: %s" % msg)


