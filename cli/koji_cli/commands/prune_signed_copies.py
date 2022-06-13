from __future__ import absolute_import, division

import fnmatch
import os
import stat
import time
from optparse import SUPPRESS_HELP, OptionParser

import six
from six.moves import range, zip

import koji
from koji.util import to_list

from koji_cli.lib import (
    activate_session,
    get_usage_str,
    warn
)


def handle_prune_signed_copies(goptions, session, args):
    "[admin] Prune signed copies"
    usage = "usage: %prog prune-signed-copies [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-n", "--test", action="store_true", help="Test mode")
    parser.add_option("-v", "--verbose", action="store_true", help="Be more verbose")
    parser.add_option("--days", type="int", default=5, help="Timeout before clearing")
    parser.add_option("-p", "--package", "--pkg", help="Limit to a single package")
    parser.add_option("-b", "--build", help="Limit to a single build")
    parser.add_option("-i", "--ignore-tag", action="append", default=[],
                      help="Ignore these tags when considering whether a build is/was latest")
    parser.add_option("--ignore-tag-file",
                      help="File to read tag ignore patterns from")
    parser.add_option("-r", "--protect-tag", action="append", default=[],
                      help="Do not prune signed copies from matching tags")
    parser.add_option("--protect-tag-file",
                      help="File to read tag protect patterns from")
    parser.add_option("--trashcan-tag", default="trashcan", help="Specify trashcan tag")
    # Don't use local debug option, this one stays here for backward compatibility
    # https://pagure.io/koji/issue/2084
    parser.add_option("--debug", action="store_true", default=goptions.debug, help=SUPPRESS_HELP)
    (options, args) = parser.parse_args(args)
    # different ideas/modes
    #  1) remove all signed copies of builds that are not latest for some tag
    #  2) remove signed copies when a 'better' signature is available
    #  3) for a specified tag, remove all signed copies that are not latest (w/ inheritance)
    #  4) for a specified tag, remove all signed copies (no inheritance)
    #     (but skip builds that are multiply tagged)

    # for now, we're just implementing mode #1
    # (with the modification that we check to see if the build was latest within
    # the last N days)
    if options.ignore_tag_file:
        with open(options.ignore_tag_file) as fo:
            options.ignore_tag.extend([line.strip() for line in fo.readlines()])
    if options.protect_tag_file:
        with open(options.protect_tag_file) as fo:
            options.protect_tag.extend([line.strip() for line in fo.readlines()])
    if options.debug:
        options.verbose = True
    cutoff_ts = time.time() - options.days * 24 * 3600
    if options.debug:
        print("Cutoff date: %s" % time.asctime(time.localtime(cutoff_ts)))
    activate_session(session, goptions)
    if not options.build:
        if options.verbose:
            print("Getting builds...")
        qopts = {
            'state': koji.BUILD_STATES['COMPLETE'],
            'queryOpts': {
                'limit': 50000,
                'offset': 0,
                'order': 'build_id',
            }
        }
        if options.package:
            pkginfo = session.getPackage(options.package)
            qopts['packageID'] = pkginfo['id']
        builds = []
        while True:
            chunk = [(b['nvr'], b) for b in session.listBuilds(**qopts)]
            if not chunk:
                break
            builds.extend(chunk)
            qopts['queryOpts']['offset'] += qopts['queryOpts']['limit']
        if options.verbose:
            print("...got %i builds" % len(builds))
        builds.sort()
    else:
        # single build
        binfo = session.getBuild(options.build)
        if not binfo:
            parser.error('No such build: %s' % options.build)
        builds = [("%(name)s-%(version)s-%(release)s" % binfo, binfo)]
    total_files = 0
    total_space = 0

    def _histline(event_id, x):
        if event_id == x['revoke_event']:
            ts = x['revoke_ts']
            fmt = "Untagged %(name)s-%(version)s-%(release)s from %(tag_name)s"
        elif event_id == x['create_event']:
            ts = x['create_ts']
            fmt = "Tagged %(name)s-%(version)s-%(release)s with %(tag_name)s"
            if x['active']:
                fmt += " [still active]"
        else:
            raise koji.GenericError("No such event: (%r, %r)" % (event_id, x))
        time_str = time.asctime(time.localtime(ts))
        return "%s: %s" % (time_str, fmt % x)
    for nvr, binfo in builds:
        # listBuilds returns slightly different data than normal
        if 'id' not in binfo:
            binfo['id'] = binfo['build_id']
        if 'name' not in binfo:
            binfo['name'] = binfo['package_name']
        if options.debug:
            print("DEBUG: %s" % nvr)
        # see how recently this build was latest for a tag
        is_latest = False
        is_protected = False
        last_latest = None
        tags = {}
        for entry in session.queryHistory(build=binfo['id'])['tag_listing']:
            # we used queryHistory rather than listTags so we can consider tags
            # that the build was recently untagged from
            tags.setdefault(entry['tag.name'], 1)
        if options.debug:
            print("Tags: %s" % to_list(tags.keys()))
        for tag_name in tags:
            if tag_name == options.trashcan_tag:
                if options.debug:
                    print("Ignoring trashcan tag for build %s" % nvr)
                continue
            ignore_tag = False
            for pattern in options.ignore_tag:
                if fnmatch.fnmatch(tag_name, pattern):
                    if options.debug:
                        print("Ignoring tag %s for build %s" % (tag_name, nvr))
                    ignore_tag = True
                    break
            if ignore_tag:
                continue
            # in order to determine how recently this build was latest, we have
            # to look at the tagging history.
            hist = session.queryHistory(tag=tag_name, package=binfo['name'])['tag_listing']
            if not hist:
                # really shouldn't happen
                raise koji.GenericError("No history found for %s in %s" % (nvr, tag_name))
            timeline = []
            for x in hist:
                # note that for revoked entries, we're effectively splitting them into
                # two parts: creation and revocation.
                timeline.append((x['create_event'], 1, x))
                # at the same event, revokes happen first
                if x['revoke_event'] is not None:
                    timeline.append((x['revoke_event'], 0, x))
            timeline.sort(key=lambda entry: entry[:2])
            # find most recent creation entry for our build and crop there
            latest_ts = None
            for i in range(len(timeline) - 1, -1, -1):
                # searching in reverse cronological order
                event_id, is_create, entry = timeline[i]
                if entry['build_id'] == binfo['id'] and is_create:
                    latest_ts = event_id
                    break
            if not latest_ts:
                # really shouldn't happen
                raise koji.GenericError("No creation event found for %s in %s" % (nvr, tag_name))
            our_entry = entry
            if options.debug:
                print(_histline(event_id, our_entry))
            # now go through the events since most recent creation entry
            timeline = timeline[i + 1:]
            if not timeline:
                is_latest = True
                if options.debug:
                    print("%s is latest in tag %s" % (nvr, tag_name))
                break
            # before we go any further, is this a protected tag?
            protect_tag = False
            for pattern in options.protect_tag:
                if fnmatch.fnmatch(tag_name, pattern):
                    protect_tag = True
                    break
            if protect_tag:
                # we use the same time limit as for the latest calculation
                # if this build was in this tag within that limit, then we will
                # not prune its signed copies
                if our_entry['revoke_event'] is None:
                    # we're still tagged with a protected tag
                    if options.debug:
                        print("Build %s has protected tag %s" % (nvr, tag_name))
                    is_protected = True
                    break
                elif our_entry['revoke_ts'] > cutoff_ts:
                    # we were still tagged here sometime before the cutoff
                    if options.debug:
                        print("Build %s had protected tag %s until %s"
                              % (nvr, tag_name,
                                 time.asctime(time.localtime(our_entry['revoke_ts']))))
                    is_protected = True
                    break
            replaced_ts = None
            revoke_ts = None
            others = {}
            for event_id, is_create, entry in timeline:
                # So two things can knock this build from the title of latest:
                #  - it could be untagged (entry revoked)
                #  - another build could become latest (replaced)
                # Note however that if the superceding entry is itself revoked, then
                # our build could become latest again
                if options.debug:
                    print(_histline(event_id, entry))
                if entry['build_id'] == binfo['id']:
                    if is_create:
                        # shouldn't happen
                        raise koji.GenericError("Duplicate creation event found for %s in %s"
                                                % (nvr, tag_name))
                    else:
                        # we've been revoked
                        revoke_ts = entry['revoke_ts']
                        break
                else:
                    if is_create:
                        # this build has become latest
                        replaced_ts = entry['create_ts']
                        if entry['active']:
                            # this entry not revoked yet, so we're done for this tag
                            break
                        # since this entry is revoked later, our build might eventually be
                        # uncovered, so we have to keep looking
                        others[entry['build_id']] = 1
                    else:
                        # other build revoked
                        # see if our build has resurfaced
                        if entry['build_id'] in others:
                            del others[entry['build_id']]
                        if replaced_ts is not None and not others:
                            # we've become latest again
                            # (note: we're not revoked yet because that triggers a break above)
                            replaced_ts = None
                            latest_ts = entry['revoke_ts']
            if last_latest is None:
                timestamps = []
            else:
                timestamps = [last_latest]
            if revoke_ts is None:
                if replaced_ts is None:
                    # turns out we are still latest
                    is_latest = True
                    if options.debug:
                        print("%s is latest (again) in tag %s" % (nvr, tag_name))
                    break
                else:
                    # replaced (but not revoked)
                    timestamps.append(replaced_ts)
                    if options.debug:
                        print("tag %s: %s not latest (replaced %s)"
                              % (tag_name, nvr, time.asctime(time.localtime(replaced_ts))))
            elif replaced_ts is None:
                # revoked but not replaced
                timestamps.append(revoke_ts)
                if options.debug:
                    print("tag %s: %s not latest (revoked %s)"
                          % (tag_name, nvr, time.asctime(time.localtime(revoke_ts))))
            else:
                # revoked AND replaced
                timestamps.append(min(revoke_ts, replaced_ts))
                if options.debug:
                    print("tag %s: %s not latest (revoked %s, replaced %s)"
                          % (tag_name, nvr, time.asctime(time.localtime(revoke_ts)),
                             time.asctime(time.localtime(replaced_ts))))
            last_latest = max(timestamps)
            if last_latest > cutoff_ts:
                if options.debug:
                    print("%s was latest past the cutoff" % nvr)
                is_latest = True
                break
        if is_latest:
            continue
        if is_protected:
            continue
        # not latest anywhere since cutoff, so we can remove all signed copies
        rpms = session.listRPMs(buildID=binfo['id'])
        session.multicall = True
        for rpminfo in rpms:
            session.queryRPMSigs(rpm_id=rpminfo['id'])
        by_sig = {}
        # index by sig
        for rpminfo, [sigs] in zip(rpms, session.multiCall()):
            for sig in sigs:
                sigkey = sig['sigkey']
                by_sig.setdefault(sigkey, []).append(rpminfo)
        builddir = koji.pathinfo.build(binfo)
        build_files = 0
        build_space = 0
        if not by_sig and options.debug:
            print("(build has no signatures)")
        for sigkey, rpms in six.iteritems(by_sig):
            mycount = 0
            archdirs = {}
            sigdirs = {}
            for rpminfo in rpms:
                signedpath = "%s/%s" % (builddir, koji.pathinfo.signed(rpminfo, sigkey))
                try:
                    st = os.lstat(signedpath)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode):
                    # warn about this
                    print("Skipping %s. Not a regular file" % signedpath)
                    continue
                if st.st_mtime > cutoff_ts:
                    print("Skipping %s. File newer than cutoff" % signedpath)
                    continue
                if options.test:
                    print("Would have unlinked: %s" % signedpath)
                else:
                    if options.verbose:
                        print("Unlinking: %s" % signedpath)
                    try:
                        os.unlink(signedpath)
                    except OSError as e:
                        print("Error removing %s: %s" % (signedpath, e))
                        print("This script needs write access to %s" % koji.BASEDIR)
                        continue
                mycount += 1
                build_files += 1
                build_space += st.st_size
                # XXX - this makes some layout assumptions, but
                #      pathinfo doesn't report what we need
                mydir = os.path.dirname(signedpath)
                archdirs[mydir] = 1
                sigdirs[os.path.dirname(mydir)] = 1
            for dir in archdirs:
                if options.test:
                    print("Would have removed dir: %s" % dir)
                else:
                    if options.verbose:
                        print("Removing dir: %s" % dir)
                    try:
                        os.rmdir(dir)
                    except OSError as e:
                        print("Error removing %s: %s" % (signedpath, e))
            if len(sigdirs) == 1:
                dir = to_list(sigdirs.keys())[0]
                if options.test:
                    print("Would have removed dir: %s" % dir)
                else:
                    if options.verbose:
                        print("Removing dir: %s" % dir)
                    try:
                        os.rmdir(dir)
                    except OSError as e:
                        print("Error removing %s: %s" % (signedpath, e))
            elif len(sigdirs) > 1:
                warn("More than one signature dir for %s: %r" % (sigkey, sigdirs))
        if build_files:
            total_files += build_files
            total_space += build_space
            if options.verbose:
                print("Build: %s, Removed %i signed copies (%i bytes). Total: %i/%i"
                      % (nvr, build_files, build_space, total_files, total_space))
        elif options.debug and by_sig:
            print("(build has no signed copies)")
    print("--- Grand Totals ---")
    print("Files: %i" % total_files)
    print("Bytes: %i" % total_space)
