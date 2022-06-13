from __future__ import absolute_import, division

import os
from optparse import OptionParser


import koji

from koji_cli.lib import (
    _progress_callback,
    _running_in_bg,
    activate_session,
    get_usage_str,
    unique_path,
    watch_tasks
)


def handle_image_build_indirection(options, session, args):
    """[build] Create a disk image using other disk images via the Indirection plugin"""
    usage = "usage: %prog image-build-indirection [base_image] [utility_image] " \
            "[indirection_build_template]"
    usage += "\n       %prog image-build --config <FILE>\n"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--config",
                      help="Use a configuration file to define image-build options "
                           "instead of command line options (they will be ignored).")
    parser.add_option("--background", action="store_true",
                      help="Run the image creation task at a lower priority")
    parser.add_option("--name", help="Name of the output image")
    parser.add_option("--version", help="Version of the output image")
    parser.add_option("--release", help="Release of the output image")
    parser.add_option("--arch", help="Architecture of the output image and input images")
    parser.add_option("--target", help="Build target to use for the indirection build")
    parser.add_option("--skip-tag", action="store_true", help="Do not tag the resulting build")
    parser.add_option("--base-image-task",
                      help="ID of the createImage task of the base image to be used")
    parser.add_option("--base-image-build", help="NVR or build ID of the base image to be used")
    parser.add_option("--utility-image-task",
                      help="ID of the createImage task of the utility image to be used")
    parser.add_option("--utility-image-build",
                      help="NVR or build ID of the utility image to be used")
    parser.add_option("--indirection-template",
                      help="Name of the local file, or SCM file containing the template used to "
                           "drive the indirection plugin")
    parser.add_option("--indirection-template-url",
                      help="SCM URL containing the template used to drive the indirection plugin")
    parser.add_option("--results-loc",
                      help="Relative path inside the working space image where the results "
                           "should be extracted from")
    parser.add_option("--scratch", action="store_true", help="Create a scratch image")
    parser.add_option("--wait", action="store_true",
                      help="Wait on the image creation, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Do not wait on the image creation")
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")

    (task_options, args) = parser.parse_args(args)
    _build_image_indirection(options, task_options, session, args)


def _build_image_indirection(options, task_opts, session, args):
    """
    A private helper function for builds using the indirection plugin of ImageFactory
    """

    # Do some sanity checks before even attempting to create the session
    if not (bool(task_opts.utility_image_task) !=
            bool(task_opts.utility_image_build)):
        raise koji.GenericError("You must specify either a utility-image task or build ID/NVR")

    if not (bool(task_opts.base_image_task) !=
            bool(task_opts.base_image_build)):
        raise koji.GenericError("You must specify either a base-image task or build ID/NVR")

    required_opts = ['name', 'version', 'arch', 'target', 'indirection_template', 'results_loc']
    optional_opts = ['indirection_template_url', 'scratch', 'utility_image_task',
                     'utility_image_build', 'base_image_task', 'base_image_build', 'release',
                     'skip_tag']

    missing = []
    for opt in required_opts:
        if not getattr(task_opts, opt, None):
            missing.append(opt)

    if len(missing) > 0:
        print("Missing the following required options: %s" %
              ' '.join(['--%s' % o.replace('_', '-') for o in missing]))
        raise koji.GenericError("Missing required options specified above")

    activate_session(session, options)

    # Set the task's priority. Users can only lower it with --background.
    priority = None
    if task_opts.background:
        # relative to koji.PRIO_DEFAULT; higher means a "lower" priority.
        priority = 5
    if _running_in_bg() or task_opts.noprogress:
        callback = None
    else:
        callback = _progress_callback

    # We do some early sanity checking of the given target.
    # Kojid gets these values again later on, but we check now as a convenience
    # for the user.

    tmp_target = session.getBuildTarget(task_opts.target)
    if not tmp_target:
        raise koji.GenericError("No such build target: %s" % tmp_target)
    dest_tag = session.getTag(tmp_target['dest_tag'])
    if not dest_tag:
        raise koji.GenericError("No such destination tag: %s" % tmp_target['dest_tag_name'])

    # Set the architecture
    task_opts.arch = koji.canonArch(task_opts.arch)

    # Upload the indirection template file to the staging area.
    # If it's a URL, it's kojid's job to go get it when it does the checkout.
    if not task_opts.indirection_template_url:
        if not task_opts.scratch:
            # only scratch builds can omit indirection_template_url
            raise koji.GenericError(
                "Non-scratch builds must provide a URL for the indirection template")
        templatefile = task_opts.indirection_template
        serverdir = unique_path('cli-image-indirection')
        session.uploadWrapper(templatefile, serverdir, callback=callback)
        task_opts.indirection_template = os.path.join('work', serverdir,
                                                      os.path.basename(templatefile))
        print('')

    hub_opts = {}
    # Just pass everything in as opts.  No posiitonal arguments at all.  Why not?
    for opt in required_opts + optional_opts:
        val = getattr(task_opts, opt, None)
        # We pass these through even if they are None
        # The builder code can then check if they are set without using getattr
        hub_opts[opt] = val

    # finally, create the task.
    task_id = session.buildImageIndirection(opts=hub_opts,
                                            priority=priority)

    if not options.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if task_opts.wait or (task_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet, topurl=options.topurl)


