from __future__ import absolute_import, division

import os
from optparse import OptionParser

import koji

from koji_cli.lib import (
    _progress_callback,
    _running_in_bg,
    activate_session,
    error,
    get_usage_str,
    linked_upload,
    unique_path
)


def handle_import_cg(goptions, session, args):
    "[admin] Import external builds with rich metadata"
    usage = "usage: %prog import-cg [options] <metadata_file> <files_dir>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")
    parser.add_option("--link", action="store_true",
                      help="Attempt to hardlink instead of uploading")
    parser.add_option("--test", action="store_true", help="Don't actually import")
    parser.add_option("--token", action="store", default=None, help="Build reservation token")
    (options, args) = parser.parse_args(args)
    if len(args) < 2:
        parser.error("Please specify metadata files directory")
    activate_session(session, goptions)
    metadata = koji.load_json(args[0])
    if 'output' not in metadata:
        error("Metadata contains no output")
    localdir = args[1]

    to_upload = []
    for info in metadata['output']:
        if info.get('metadata_only', False):
            continue
        localpath = os.path.join(localdir, info.get('relpath', ''), info['filename'])
        if not os.path.exists(localpath):
            parser.error("No such file: %s" % localpath)
        to_upload.append([localpath, info])

    if options.test:
        return

    # get upload path
    # XXX - need a better way
    serverdir = unique_path('cli-import')

    for localpath, info in to_upload:
        relpath = os.path.join(serverdir, info.get('relpath', ''))
        if _running_in_bg() or options.noprogress:
            callback = None
        else:
            callback = _progress_callback
        if options.link:
            linked_upload(localpath, relpath)
        else:
            print("Uploading %s" % localpath)
            session.uploadWrapper(localpath, relpath, callback=callback)
            if callback:
                print('')

    session.CGImport(metadata, serverdir, options.token)
