from __future__ import absolute_import, division

import os
from optparse import SUPPRESS_HELP, OptionParser


import koji
from koji.util import base64encode, md5_constructor

from koji_cli.lib import (
    activate_session,
    get_usage_str,
    warn
)


def handle_import_sig(goptions, session, args):
    "[admin] Import signatures into the database and write signed RPMs"
    usage = "usage: %prog import-sig [options] <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--with-unsigned", action="store_true",
                      help="Also import unsigned sig headers")
    parser.add_option("--write", action="store_true", help=SUPPRESS_HELP)
    parser.add_option("--test", action="store_true", help="Test mode -- don't actually import")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("At least one package must be specified")
    for path in args:
        if not os.path.exists(path):
            parser.error("No such file: %s" % path)
    activate_session(session, goptions)
    for path in args:
        data = koji.get_header_fields(path, ('name', 'version', 'release', 'arch', 'siggpg',
                                             'sigpgp', 'dsaheader', 'rsaheader',
                                             'sourcepackage'))
        if data['sourcepackage']:
            data['arch'] = 'src'
        sigkey = data['siggpg']
        if not sigkey:
            sigkey = data['sigpgp']
        if not sigkey:
            sigkey = data['dsaheader']
        if not sigkey:
            sigkey = data['rsaheader']
        if not sigkey:
            sigkey = ""
            if not options.with_unsigned:
                print("Skipping unsigned package: %s" % path)
                continue
        else:
            sigkey = koji.get_sigpacket_key_id(sigkey)
        del data['siggpg']
        del data['sigpgp']
        del data['dsaheader']
        del data['rsaheader']
        rinfo = session.getRPM(data)
        if not rinfo:
            print("No such rpm in system: %(name)s-%(version)s-%(release)s.%(arch)s" % data)
            continue
        if rinfo.get('external_repo_id'):
            print("Skipping external rpm: %(name)s-%(version)s-%(release)s.%(arch)s@"
                  "%(external_repo_name)s" % rinfo)
            continue
        sighdr = koji.rip_rpm_sighdr(path)
        previous = session.queryRPMSigs(rpm_id=rinfo['id'], sigkey=sigkey)
        assert len(previous) <= 1
        if previous:
            sighash = md5_constructor(sighdr).hexdigest()
            if previous[0]['sighash'] == sighash:
                print("Signature already imported: %s" % path)
                continue
            else:
                warn("signature mismatch: %s" % path)
                warn("  The system already has a signature for this rpm with key %s" % sigkey)
                warn("  The two signature headers are not the same")
                continue
        print("Importing signature [key %s] from %s..." % (sigkey, path))
        if not options.test:
            session.addRPMSig(rinfo['id'], base64encode(sighdr))
        print("Writing signed copy")
        if not options.test:
            session.writeSignedRPM(rinfo['id'], sigkey)
