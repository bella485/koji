from __future__ import absolute_import, division

import os
from optparse import OptionParser


import koji

from koji_cli.lib import (
    _progress_callback,
    _running_in_bg,
    activate_session,
    arg_filter,
    get_usage_str,
    linked_upload,
    unique_path
)


def handle_import_archive(options, session, args):
    "[admin] Import an archive file and associate it with a build"
    usage = "usage: %prog import-archive <build-id|n-v-r> <archive_path> [<archive_path2 ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")
    parser.add_option("--create-build", action="store_true", help="Auto-create builds as needed")
    parser.add_option("--link", action="store_true",
                      help="Attempt to hardlink instead of uploading")
    parser.add_option("--type",
                      help="The type of archive being imported. "
                           "Currently supported types: maven, win, image")
    parser.add_option("--type-info",
                      help="Type-specific information to associate with the archives. "
                           "For Maven archives this should be a local path to a .pom file. "
                           "For Windows archives this should be relpath:platforms[:flags])) "
                           "Images need an arch")
    (suboptions, args) = parser.parse_args(args)

    if not len(args) > 1:
        parser.error("You must specify a build ID or N-V-R and an archive to import")

    activate_session(session, options)

    if not suboptions.type:
        parser.error("You must specify an archive type")
    if suboptions.type == 'maven':
        if not (session.hasPerm('maven-import') or session.hasPerm('admin')):
            parser.error("This action requires the maven-import privilege")
        if not suboptions.type_info:
            parser.error("--type-info must point to a .pom file when importing Maven archives")
        pom_info = koji.parse_pom(suboptions.type_info)
        maven_info = koji.pom_to_maven_info(pom_info)
        suboptions.type_info = maven_info
    elif suboptions.type == 'win':
        if not (session.hasPerm('win-import') or session.hasPerm('admin')):
            parser.error("This action requires the win-import privilege")
        if not suboptions.type_info:
            parser.error("--type-info must be specified")
        type_info = suboptions.type_info.split(':', 2)
        if len(type_info) < 2:
            parser.error("--type-info must be in relpath:platforms[:flags] format")
        win_info = {'relpath': type_info[0], 'platforms': type_info[1].split()}
        if len(type_info) > 2:
            win_info['flags'] = type_info[2].split()
        else:
            win_info['flags'] = []
        suboptions.type_info = win_info
    elif suboptions.type == 'image':
        if not (session.hasPerm('image-import') or session.hasPerm('admin')):
            parser.error("This action requires the image-import privilege")
        if not suboptions.type_info:
            parser.error("--type-info must be specified")
        image_info = {'arch': suboptions.type_info}
        suboptions.type_info = image_info
    else:
        parser.error("Unsupported archive type: %s" % suboptions.type)

    buildinfo = session.getBuild(arg_filter(args[0]))
    if not buildinfo:
        if not suboptions.create_build:
            parser.error("No such build: %s" % args[0])
        buildinfo = koji.parse_NVR(args[0])
        if buildinfo['epoch'] == '':
            buildinfo['epoch'] = None
        else:
            buildinfo['epoch'] = int(buildinfo['epoch'])
        if suboptions.type == 'maven':
            # --type-info should point to a local .pom file
            session.createMavenBuild(buildinfo, suboptions.type_info)
        elif suboptions.type == 'win':
            # We're importing, so we don't know what platform the build
            # was run on.  Use "import" as a placeholder.
            session.createWinBuild(buildinfo, {'platform': 'import'})
        elif suboptions.type == 'image':
            # --type-info should have an arch of the image
            session.createImageBuild(buildinfo)
        else:
            # should get caught above
            assert False  # pragma: no cover

    for filepath in args[1:]:
        filename = os.path.basename(filepath)
        print("Uploading archive: %s" % filename)
        serverdir = unique_path('cli-import')
        if _running_in_bg() or suboptions.noprogress:
            callback = None
        else:
            callback = _progress_callback
        if suboptions.link:
            linked_upload(filepath, serverdir)
        else:
            session.uploadWrapper(filepath, serverdir, callback=callback)
        print('')
        serverpath = "%s/%s" % (serverdir, filename)
        session.importArchive(serverpath, buildinfo, suboptions.type, suboptions.type_info)
        print("Imported: %s" % filename)


