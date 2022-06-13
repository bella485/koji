from __future__ import absolute_import, division

import sys
import time
from collections import OrderedDict, defaultdict
from optparse import OptionParser

import six
import six.moves.xmlrpc_client
from six.moves import zip

import koji

from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)


def handle_clone_tag(goptions, session, args):
    "[admin] Duplicate the contents of one tag onto another tag"
    usage = "usage: %prog clone-tag [options] <src-tag> <dst-tag>"
    usage += "\nclone-tag will create the destination tag if it does not already exist"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option('--config', action='store_true',
                      help="Copy config from the source to the dest tag")
    parser.add_option('--groups', action='store_true', help="Copy group information")
    parser.add_option('--pkgs', action='store_true',
                      help="Copy package list from the source to the dest tag")
    parser.add_option('--builds', action='store_true', help="Tag builds into the dest tag")
    parser.add_option('--all', action='store_true',
                      help="The same as --config --groups --pkgs --builds")
    parser.add_option('--latest-only', action='store_true',
                      help="Tag only the latest build of each package")
    parser.add_option('--inherit-builds', action='store_true',
                      help="Include all builds inherited into the source tag into the dest tag")
    parser.add_option('--ts', type='int', metavar="TIMESTAMP",
                      help='Clone tag at last event before specific timestamp')
    parser.add_option('--no-delete', action='store_false', dest="delete", default=True,
                      help="Don't delete any existing content in dest tag.")
    parser.add_option('--event', type='int', help='Clone tag at a specific event')
    parser.add_option('--repo', type='int', help='Clone tag at a specific repo event')
    parser.add_option("-v", "--verbose", action="store_true", help="show changes")
    parser.add_option("--notify", action="store_true", default=False,
                      help='Send tagging/untagging notifications')
    parser.add_option("-f", "--force", action="store_true",
                      help="override tag locks if necessary")
    parser.add_option("-n", "--test", action="store_true", help="test mode")
    parser.add_option("--batch", type='int', default=100, metavar='SIZE',
                      help="batch size of multicalls [0 to disable, default: %default]")
    (options, args) = parser.parse_args(args)

    if len(args) != 2:
        parser.error("This command takes two arguments: <src-tag> <dst-tag>")
    activate_session(session, goptions)

    if not options.test and not (session.hasPerm('admin') or session.hasPerm('tag')):
        parser.error("This action requires tag or admin privileges")

    if args[0] == args[1]:
        parser.error('Source and destination tags must be different.')

    if options.batch < 0:
        parser.error("batch size must be bigger than zero")

    if options.all:
        options.config = options.groups = options.pkgs = options.builds = True

    event = koji.util.eventFromOpts(session, options) or {}
    if event:
        event['timestr'] = time.asctime(time.localtime(event['ts']))
        print("Cloning at event %(id)i (%(timestr)s)" % event)

    # store tags.
    try:
        srctag = session.getBuildConfig(args[0], event=event.get('id'))
    except koji.GenericError:
        parser.error("No such src-tag: %s" % args[0])
    dsttag = session.getTag(args[1])
    if not srctag:
        parser.error("No such src-tag: %s" % args[0])
    if (srctag['locked'] and not options.force) \
            or (dsttag and dsttag['locked'] and not options.force):
        parser.error("Error: You are attempting to clone from or to a tag which is locked.\n"
                     "Please use --force if this is what you really want to do.")

    # init debug lists.
    chgpkglist = []
    chgbldlist = []
    chggrplist = []
    # case of brand new dst-tag.
    if not dsttag:
        # create a new tag, copy srctag header.
        if not options.test:
            if options.config:
                session.createTag(args[1], parent=None, arches=srctag['arches'],
                                  perm=srctag['perm_id'],
                                  locked=srctag['locked'],
                                  maven_support=srctag['maven_support'],
                                  maven_include_all=srctag['maven_include_all'],
                                  extra=srctag['extra'])
            else:
                session.createTag(args[1], parent=None)
            # store the new tag, need its assigned id.
            newtag = session.getTag(args[1], strict=True)
        # get pkglist of src-tag, including inherited packages.
        if options.pkgs:
            srcpkgs = session.listPackages(tagID=srctag['id'],
                                           inherited=True,
                                           event=event.get('id'))
            srcpkgs.sort(key=lambda x: x['package_name'])
            if not options.test:
                session.multicall = True
            for pkgs in srcpkgs:
                # for each package add one entry in the new tag.
                chgpkglist.append(('[new]',
                                   pkgs['package_name'],
                                   pkgs['blocked'],
                                   pkgs['owner_name'],
                                   pkgs['tag_name']))
                if not options.test:
                    # add packages.
                    session.packageListAdd(newtag['name'],
                                           pkgs['package_name'],
                                           owner=pkgs['owner_name'],
                                           block=pkgs['blocked'],
                                           extra_arches=pkgs['extra_arches'])
            if not options.test:
                _multicall_with_check(session, options.batch)
        if options.builds:
            # get --all latest builds from src tag
            builds = reversed(session.listTagged(srctag['id'],
                                                 event=event.get('id'),
                                                 inherit=options.inherit_builds,
                                                 latest=options.latest_only))
            if not options.test:
                session.multicall = True
            for build in builds:
                # add missing 'name' field.
                build['name'] = build['package_name']
                chgbldlist.append(('[new]',
                                   build['package_name'],
                                   build['nvr'],
                                   koji.BUILD_STATES[build['state']],
                                   build['owner_name'],
                                   build['tag_name']))
                # copy latest builds into new tag
                if not options.test:
                    session.tagBuildBypass(newtag['name'],
                                           build,
                                           force=options.force,
                                           notify=options.notify)
            if not options.test:
                _multicall_with_check(session, options.batch)
        if options.groups:
            # Copy the group data
            srcgroups = session.getTagGroups(srctag['name'],
                                             event=event.get('id'))
            if not options.test:
                session.multicall = True
            for group in srcgroups:
                if not options.test:
                    session.groupListAdd(newtag['name'], group['name'])
                for pkg in group['packagelist']:
                    if not options.test:
                        session.groupPackageListAdd(newtag['name'],
                                                    group['name'],
                                                    pkg['package'],
                                                    block=pkg['blocked'])
                    chggrplist.append(('[new]', pkg['package'], group['name']))
            if not options.test:
                _multicall_with_check(session, options.batch)
    # case of existing dst-tag.
    if dsttag:
        if options.config and not options.test:
            if dsttag['extra']:
                session.editTag2(dsttag['id'], remove_extra=list(dsttag['extra'].keys()))
            session.editTag2(dsttag['id'], parent=None, arches=srctag['arches'],
                             perm=srctag['perm_id'],
                             locked=srctag['locked'],
                             maven_support=srctag['maven_support'],
                             maven_include_all=srctag['maven_include_all'],
                             extra=srctag['extra'])
            dsttag = session.getTag(dsttag['id'], strict=True)

        # get fresh list of packages & builds into maps.
        srcpkgs = {}
        dstpkgs = {}
        srcbldsbypkg = defaultdict(OrderedDict)
        dstbldsbypkg = defaultdict(OrderedDict)
        srcgroups = OrderedDict()
        dstgroups = OrderedDict()
        # we use OrderedDict so that these indexes preserve the order given to us
        if options.pkgs:
            for pkg in session.listPackages(tagID=srctag['id'],
                                            inherited=True,
                                            event=event.get('id')):
                srcpkgs[pkg['package_name']] = pkg
            for pkg in session.listPackages(tagID=dsttag['id'],
                                            inherited=True):
                dstpkgs[pkg['package_name']] = pkg
        if options.builds:
            # listTagged orders builds latest-first
            # so reversing that gives us oldest-first
            for build in reversed(session.listTagged(srctag['id'],
                                                     event=event.get('id'),
                                                     inherit=options.inherit_builds,
                                                     latest=options.latest_only)):
                srcbldsbypkg[build['package_name']][build['nvr']] = build
            # get builds in dsttag without inheritance.
            # latest=False to get all builds even when latest_only = True,
            # so that only the *latest* build per tag will live in.
            for build in reversed(session.listTagged(dsttag['id'],
                                                     inherit=False,
                                                     latest=False)):
                dstbldsbypkg[build['package_name']][build['nvr']] = build
        if options.groups:
            for group in session.getTagGroups(srctag['name'],
                                              event=event.get('id')):
                srcgroups[group['name']] = group
            for group in session.getTagGroups(dsttag['name']):
                dstgroups[group['name']] = group
        # construct to-do lists.
        paddlist = []  # list containing new packages to be added from src tag
        for (package_name, pkg) in six.iteritems(srcpkgs):
            if package_name not in dstpkgs:
                paddlist.append(pkg)
        paddlist.sort(key=lambda x: x['package_name'])
        pdellist = []  # list containing packages no more present in dst tag
        for (package_name, pkg) in six.iteritems(dstpkgs):
            if package_name not in srcpkgs:
                pdellist.append(pkg)
        pdellist.sort(key=lambda x: x['package_name'])
        baddlist = []  # list containing new builds to be added from src tag
        bdellist = []  # list containing new builds to be removed from dst tag
        if options.delete:
            # remove builds for packages that are absent from src tag
            for (pkg, dstblds) in six.iteritems(dstbldsbypkg):
                if pkg not in srcbldsbypkg:
                    bdellist.extend(dstblds.values())
        # add and/or remove builds from dst to match src contents and order
        for (pkg, srcblds) in six.iteritems(srcbldsbypkg):
            dstblds = dstbldsbypkg[pkg]
            ablds = []
            dblds = []
            # firstly, deal with extra builds in dst
            removed_nvrs = set(dstblds.keys()) - set(srcblds.keys())
            bld_order = srcblds.copy()
            if options.delete:
                # mark the extra builds for deletion
                dnvrs = []
                for (dstnvr, dstbld) in six.iteritems(dstblds):
                    if dstnvr in removed_nvrs:
                        dnvrs.append(dstnvr)
                        dblds.append(dstbld)
                # we also remove them from dstblds now so that they do not
                # interfere with the order comparison below
                for dnvr in dnvrs:
                    del dstblds[dnvr]
            else:
                # in the no-delete case, the extra builds should be forced
                # to last in the tag
                bld_order = OrderedDict()
                for (dstnvr, dstbld) in six.iteritems(dstblds):
                    if dstnvr in removed_nvrs:
                        bld_order[dstnvr] = dstbld
                for (nvr, srcbld) in six.iteritems(srcblds):
                    bld_order[nvr] = srcbld
            # secondly, add builds from src tag and adjust the order
            for (nvr, srcbld) in six.iteritems(bld_order):
                found = False
                out_of_order = []
                # note that dstblds is trimmed as we go, so we are only
                # considering the tail corresponding to where we are at
                # in the srcblds loop
                for (dstnvr, dstbld) in six.iteritems(dstblds):
                    if nvr == dstnvr:
                        found = True
                        break
                    else:
                        out_of_order.append(dstnvr)
                        dblds.append(dstbld)
                for dnvr in out_of_order:
                    del dstblds[dnvr]
                    # these will be re-added in the proper order later
                if found:
                    # remove it for next pass so we stay aligned with outer
                    # loop
                    del dstblds[nvr]
                else:
                    # missing from dst, so we need to add it
                    ablds.append(srcbld)
            baddlist.extend(ablds)
            bdellist.extend(dblds)
        baddlist.sort(key=lambda x: x['package_name'])
        bdellist.sort(key=lambda x: x['package_name'])

        gaddlist = []  # list containing new groups to be added from src tag
        for (grpname, group) in six.iteritems(srcgroups):
            if grpname not in dstgroups:
                gaddlist.append(group)
        gdellist = []  # list containing groups to be removed from src tag
        for (grpname, group) in six.iteritems(dstgroups):
            if grpname not in srcgroups:
                gdellist.append(group)
        grpchanges = OrderedDict()  # dict of changes to make in shared groups
        for (grpname, group) in six.iteritems(srcgroups):
            if grpname in dstgroups:
                dstgroup = dstgroups[grpname]
                grpchanges[grpname] = {'adds': [], 'dels': []}
                # Store whether group is inherited or not
                grpchanges[grpname]['inherited'] = False
                if dstgroup['tag_id'] != dsttag['id']:
                    grpchanges[grpname]['inherited'] = True
                srcgrppkglist = []
                dstgrppkglist = []
                for pkg in group['packagelist']:
                    srcgrppkglist.append(pkg['package'])
                for pkg in dstgroups[grpname]['packagelist']:
                    dstgrppkglist.append(pkg['package'])
                for pkg in srcgrppkglist:
                    if pkg not in dstgrppkglist:
                        grpchanges[grpname]['adds'].append(pkg)
                for pkg in dstgrppkglist:
                    if pkg not in srcgrppkglist:
                        grpchanges[grpname]['dels'].append(pkg)
        # ADD new packages.
        if not options.test:
            session.multicall = True
        for pkg in paddlist:
            chgpkglist.append(('[add]',
                               pkg['package_name'],
                               pkg['blocked'],
                               pkg['owner_name'],
                               pkg['tag_name']))
            if not options.test:
                session.packageListAdd(dsttag['name'],
                                       pkg['package_name'],
                                       owner=pkg['owner_name'],
                                       block=pkg['blocked'],
                                       extra_arches=pkg['extra_arches'])
        if not options.test:
            _multicall_with_check(session, options.batch)
        # DEL builds. To keep the order we should untag builds at first
        if not options.test:
            session.multicall = True
        for build in bdellist:
            # don't delete an inherited build.
            if build['tag_name'] == dsttag['name']:
                # add missing 'name' field
                build['name'] = build['package_name']
                chgbldlist.append(('[del]',
                                   build['package_name'],
                                   build['nvr'],
                                   koji.BUILD_STATES[build['state']],
                                   build['owner_name'],
                                   build['tag_name']))
                # go on del builds from new tag.
                if not options.test:
                    session.untagBuildBypass(dsttag['name'],
                                             build,
                                             force=options.force,
                                             notify=options.notify)
        if not options.test:
            _multicall_with_check(session, options.batch)
        # ADD builds.
        if not options.test:
            session.multicall = True
        for build in baddlist:
            # add missing 'name' field.
            build['name'] = build['package_name']
            chgbldlist.append(('[add]',
                               build['package_name'],
                               build['nvr'],
                               koji.BUILD_STATES[build['state']],
                               build['owner_name'],
                               build['tag_name']))
            # copy latest builds into new tag.
            if not options.test:
                session.tagBuildBypass(dsttag['name'],
                                       build,
                                       force=options.force,
                                       notify=options.notify)
        if not options.test:
            _multicall_with_check(session, options.batch)
        # ADD groups.
        if not options.test:
            session.multicall = True
        for group in gaddlist:
            if not options.test:
                session.groupListAdd(dsttag['name'],
                                     group['name'],
                                     force=options.force)
            for pkg in group['packagelist']:
                if not options.test:
                    session.groupPackageListAdd(dsttag['name'],
                                                group['name'],
                                                pkg['package'],
                                                force=options.force)
                chggrplist.append(('[new]', pkg['package'], group['name']))
        if not options.test:
            _multicall_with_check(session, options.batch)
        # ADD group pkgs.
        if not options.test:
            session.multicall = True
        for group in grpchanges:
            for pkg in grpchanges[group]['adds']:
                chggrplist.append(('[new]', pkg, group))
                if not options.test:
                    session.groupPackageListAdd(dsttag['name'],
                                                group,
                                                pkg,
                                                force=options.force)
        if not options.test:
            _multicall_with_check(session, options.batch)
        if options.delete:
            # DEL packages
            ninhrtpdellist = []
            inhrtpdellist = []
            for pkg in pdellist:
                if pkg['tag_name'] == dsttag['name']:
                    ninhrtpdellist.append(pkg)
                else:
                    inhrtpdellist.append(pkg)
            session.multicall = True
            # delete only non-inherited packages.
            for pkg in ninhrtpdellist:
                # check if package have owned builds inside.
                session.listTagged(dsttag['name'],
                                   package=pkg['package_name'],
                                   inherit=False)
            bump_builds = session.multiCall(batch=options.batch)
            if not options.test:
                session.multicall = True
            for pkg, [builds] in zip(ninhrtpdellist, bump_builds):
                if isinstance(builds, dict):
                    error(builds['faultString'])
                # remove all its builds first if there are any.
                for build in builds:
                    # add missing 'name' field.
                    build['name'] = build['package_name']
                    chgbldlist.append(('[del]',
                                       build['package_name'],
                                       build['nvr'],
                                       koji.BUILD_STATES[build['state']],
                                       build['owner_name'],
                                       build['tag_name']))
                    # so delete latest build(s) from new tag.
                    if not options.test:
                        session.untagBuildBypass(dsttag['name'],
                                                 build,
                                                 force=options.force,
                                                 notify=options.notify)
                # now safe to remove package itself since we resolved its builds.
                chgpkglist.append(('[del]',
                                   pkg['package_name'],
                                   pkg['blocked'],
                                   pkg['owner_name'],
                                   pkg['tag_name']))
                if not options.test:
                    session.packageListRemove(dsttag['name'],
                                              pkg['package_name'],
                                              force=False)
            # mark as blocked inherited packages.
            for pkg in inhrtpdellist:
                chgpkglist.append(('[blk]',
                                   pkg['package_name'],
                                   pkg['blocked'],
                                   pkg['owner_name'],
                                   pkg['tag_name']))
                if not options.test:
                    session.packageListBlock(dsttag['name'], pkg['package_name'])
            if not options.test:
                _multicall_with_check(session, options.batch)
            # DEL groups.
            if not options.test:
                session.multicall = True
            for group in gdellist:
                # Only delete a group that isn't inherited
                if group['tag_id'] == dsttag['id']:
                    if not options.test:
                        session.groupListRemove(dsttag['name'],
                                                group['name'],
                                                force=options.force)
                    for pkg in group['packagelist']:
                        chggrplist.append(('[del]', pkg['package'], group['name']))
                # mark as blocked inherited groups.
                else:
                    if not options.test:
                        session.groupListBlock(dsttag['name'], group['name'])
                    for pkg in group['packagelist']:
                        chggrplist.append(('[blk]', pkg['package'], group['name']))
            if not options.test:
                _multicall_with_check(session, options.batch)
            # DEL group pkgs.
            if not options.test:
                session.multicall = True
            for group in grpchanges:
                for pkg in grpchanges[group]['dels']:
                    # Only delete a group that isn't inherited
                    if not grpchanges[group]['inherited']:
                        chggrplist.append(('[del]', pkg, group))
                        if not options.test:
                            session.groupPackageListRemove(dsttag['name'],
                                                           group,
                                                           pkg,
                                                           force=options.force)
                    else:
                        chggrplist.append(('[blk]', pkg, group))
                        if not options.test:
                            session.groupPackageListBlock(dsttag['name'],
                                                          group,
                                                          pkg)
            if not options.test:
                _multicall_with_check(session, options.batch)
    # print final list of actions.
    if options.verbose:
        pfmt = '    %-7s %-28s %-10s %-10s %-10s\n'
        bfmt = '    %-7s %-28s %-40s %-10s %-10s %-10s\n'
        gfmt = '    %-7s %-28s %-28s\n'
        sys.stdout.write('\nList of changes:\n\n')
        sys.stdout.write(pfmt % ('Action', 'Package', 'Blocked', 'Owner', 'From Tag'))
        sys.stdout.write(pfmt % ('-' * 7, '-' * 28, '-' * 10, '-' * 10, '-' * 10))
        for changes in chgpkglist:
            sys.stdout.write(pfmt % changes)
        sys.stdout.write('\n')
        sys.stdout.write(bfmt %
                         ('Action', 'From/To Package', 'Build(s)', 'State', 'Owner', 'From Tag'))
        sys.stdout.write(bfmt % ('-' * 7, '-' * 28, '-' * 40, '-' * 10, '-' * 10, '-' * 10))
        for changes in chgbldlist:
            sys.stdout.write(bfmt % changes)
        sys.stdout.write('\n')
        sys.stdout.write(gfmt % ('Action', 'Package', 'Group'))
        sys.stdout.write(gfmt % ('-' * 7, '-' * 28, '-' * 28))
        for changes in chggrplist:
            sys.stdout.write(gfmt % changes)


