from __future__ import absolute_import, division

import os
import sys
from optparse import OptionParser

import koji
from koji.util import to_list

from koji_cli.lib import (
    activate_session,
    get_usage_str,
    linked_upload,
    unique_path,
    warn
)


def handle_import(goptions, session, args):
    "[admin] Import externally built RPMs into the database"
    usage = "usage: %prog import [options] <package> [<package> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--link", action="store_true",
                      help="Attempt to hardlink instead of uploading")
    parser.add_option("--test", action="store_true", help="Don't actually import")
    parser.add_option("--create-build", action="store_true", help="Auto-create builds as needed")
    parser.add_option("--src-epoch", help="When auto-creating builds, use this epoch")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("At least one package must be specified")
    if options.src_epoch in ('None', 'none', '(none)'):
        options.src_epoch = None
    elif options.src_epoch:
        try:
            options.src_epoch = int(options.src_epoch)
        except (ValueError, TypeError):
            parser.error("Invalid value for epoch: %s" % options.src_epoch)
    activate_session(session, goptions)
    to_import = {}
    for path in args:
        data = koji.get_header_fields(path, ('name', 'version', 'release', 'epoch',
                                             'arch', 'sigmd5', 'sourcepackage', 'sourcerpm'))
        if data['sourcepackage']:
            data['arch'] = 'src'
            nvr = "%(name)s-%(version)s-%(release)s" % data
        else:
            nvr = "%(name)s-%(version)s-%(release)s" % koji.parse_NVRA(data['sourcerpm'])
        to_import.setdefault(nvr, []).append((path, data))
    builds_missing = False
    nvrs = sorted(to_list(to_import.keys()))
    for nvr in nvrs:
        to_import[nvr].sort()
        for path, data in to_import[nvr]:
            if data['sourcepackage']:
                break
        else:
            # no srpm included, check for build
            binfo = session.getBuild(nvr)
            if not binfo:
                print("Missing build or srpm: %s" % nvr)
                builds_missing = True
    if builds_missing and not options.create_build:
        print("Aborting import")
        return

    # local function to help us out below
    def do_import(path, data):
        rinfo = dict([(k, data[k]) for k in ('name', 'version', 'release', 'arch')])
        prev = session.getRPM(rinfo)
        if prev and not prev.get('external_repo_id', 0):
            if prev['payloadhash'] == koji.hex_string(data['sigmd5']):
                print("RPM already imported: %s" % path)
            else:
                warn("md5sum mismatch for %s" % path)
                warn("  A different rpm with the same name has already been imported")
                warn("  Existing sigmd5 is %r, your import has %r" % (
                    prev['payloadhash'], koji.hex_string(data['sigmd5'])))
            print("Skipping import")
            return
        if options.test:
            print("Test mode -- skipping import for %s" % path)
            return
        serverdir = unique_path('cli-import')
        if options.link:
            linked_upload(path, serverdir)
        else:
            sys.stdout.write("uploading %s... " % path)
            sys.stdout.flush()
            session.uploadWrapper(path, serverdir)
            print("done")
            sys.stdout.flush()
        sys.stdout.write("importing %s... " % path)
        sys.stdout.flush()
        try:
            session.importRPM(serverdir, os.path.basename(path))
        except koji.GenericError as e:
            print("\nError importing: %s" % str(e).splitlines()[-1])
            sys.stdout.flush()
        else:
            print("done")
        sys.stdout.flush()

    for nvr in nvrs:
        # check for existing build
        need_build = True
        binfo = session.getBuild(nvr)
        if binfo:
            b_state = koji.BUILD_STATES[binfo['state']]
            if b_state == 'COMPLETE':
                need_build = False
            elif b_state in ['FAILED', 'CANCELED']:
                if not options.create_build:
                    print("Build %s state is %s. Skipping import" % (nvr, b_state))
                    continue
            else:
                print("Build %s exists with state=%s. Skipping import" % (nvr, b_state))
                continue

        # import srpms first, if any
        for path, data in to_import[nvr]:
            if data['sourcepackage']:
                if binfo and b_state != 'COMPLETE':
                    # need to fix the state
                    print("Creating empty build: %s" % nvr)
                    b_data = koji.util.dslice(binfo, ['name', 'version', 'release'])
                    b_data['epoch'] = data['epoch']
                    session.createEmptyBuild(**b_data)
                    binfo = session.getBuild(nvr)
                do_import(path, data)
                need_build = False

        if need_build:
            # if we're doing this here, we weren't given the matching srpm
            if not options.create_build:  # pragma: no cover
                if binfo:
                    # should have caught this earlier, but just in case...
                    b_state = koji.BUILD_STATES[binfo['state']]
                    print("Build %s state is %s. Skipping import" % (nvr, b_state))
                    continue
                else:
                    print("No such build: %s (include matching srpm or use "
                          "--create-build option to add it)" % nvr)
                    continue
            else:
                # let's make a new build
                b_data = koji.parse_NVR(nvr)
                if options.src_epoch:
                    b_data['epoch'] = options.src_epoch
                else:
                    # pull epoch from first rpm
                    data = to_import[nvr][0][1]
                    b_data['epoch'] = data['epoch']
                if options.test:
                    print("Test mode -- would have created empty build: %s" % nvr)
                else:
                    print("Creating empty build: %s" % nvr)
                    session.createEmptyBuild(**b_data)
                    binfo = session.getBuild(nvr)

        for path, data in to_import[nvr]:
            if data['sourcepackage']:
                continue
            do_import(path, data)
