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


def handle_image_build(options, session, args):
    """[build] Create a disk image given an install tree"""
    formats = ('vmdk', 'qcow', 'qcow2', 'vdi', 'vpc', 'rhevm-ova',
               'vsphere-ova', 'vagrant-virtualbox', 'vagrant-libvirt',
               'vagrant-vmware-fusion', 'vagrant-hyperv', 'docker', 'raw-xz',
               'liveimg-squashfs', 'tar-gz')
    usage = "usage: %prog image-build [options] <name> <version> <target> " \
            "<install-tree-url> <arch> [<arch> ...]"
    usage += "\n       %prog image-build --config <FILE>\n"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--background", action="store_true",
                      help="Run the image creation task at a lower priority")
    parser.add_option("--config",
                      help="Use a configuration file to define image-build options "
                           "instead of command line options (they will be ignored).")
    parser.add_option("--disk-size", default=10, help="Set the disk device size in gigabytes")
    parser.add_option("--distro",
                      help="specify the RPM based distribution the image will be based "
                           "on with the format RHEL-X.Y, CentOS-X.Y, SL-X.Y, or Fedora-NN. "
                           "The packages for the Distro you choose must have been built "
                           "in this system.")
    parser.add_option("--format", default=[], action="append",
                      help="Convert results to one or more formats "
                           "(%s), this option may be used "
                           "multiple times. By default, specifying this option will "
                           "omit the raw disk image (which is 10G in size) from the "
                           "build results. If you really want it included with converted "
                           "images, pass in 'raw' as an option." % ', '.join(formats))
    parser.add_option("--kickstart", help="Path to a local kickstart file")
    parser.add_option("--ksurl", metavar="SCMURL",
                      help="The URL to the SCM containing the kickstart file")
    parser.add_option("--ksversion", metavar="VERSION",
                      help="The syntax version used in the kickstart file")
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")
    parser.add_option("--noverifyssl", action="store_true",
                      help="Use the noverifyssl option for the install tree and all repos. "
                           "This option is only allowed if enabled on the builder.")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Don't wait on image creation")
    parser.add_option("--ova-option", action="append",
                      help="Override a value in the OVA description XML. Provide a value "
                           "in a name=value format, such as 'ovf_memory_mb=6144'")
    parser.add_option("--factory-parameter", nargs=2, action="append",
                      help="Pass a parameter to Image Factory. The results are highly specific "
                           "to the image format being created. This is a two argument parameter "
                           "that can be specified an arbitrary number of times. For example: "
                           "--factory-parameter docker_cmd '[ \"/bin/echo Hello World\" ]'")
    parser.add_option("--release", help="Forcibly set the release field")
    parser.add_option("--repo", action="append",
                      help="Specify a repo that will override the repo used to install "
                           "RPMs in the image. May be used multiple times. The "
                           "build tag repo associated with the target is the default.")
    parser.add_option("--scratch", action="store_true", help="Create a scratch image")
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag package")
    parser.add_option("--can-fail", action="store", dest="optional_arches",
                      metavar="ARCH1,ARCH2,...", default="",
                      help="List of archs which are not blocking for build (separated by commas.")
    parser.add_option("--specfile", metavar="URL",
                      help="SCM URL to spec file fragment to use to generate wrapper RPMs")
    parser.add_option("--wait", action="store_true",
                      help="Wait on the image creation, even if running in the background")

    (task_options, args) = parser.parse_args(args)

    if task_options.config:
        section = 'image-build'
        config = koji.read_config_files([(task_options.config, True)])
        if not config.has_section(section):
            parser.error("single section called [%s] is required" % section)
        # pluck out the positional arguments first
        args = []
        for arg in ('name', 'version', 'target', 'install_tree'):
            args.append(config.get(section, arg))
            config.remove_option(section, arg)
        args.extend(config.get(section, 'arches').split(','))
        config.remove_option(section, 'arches')
        # turn comma-separated options into lists
        for arg in ('repo', 'format'):
            if config.has_option(section, arg):
                setattr(task_options, arg, config.get(section, arg).split(','))
                config.remove_option(section, arg)
        if config.has_option(section, 'can_fail'):
            setattr(task_options, 'optional_arches', config.get(section, 'can_fail').split(','))
            config.remove_option(section, 'can_fail')
        # handle everything else
        for k, v in config.items(section):
            setattr(task_options, k, v)

        # ova-options belong in their own section
        section = 'ova-options'
        if config.has_section(section):
            task_options.ova_option = []
            for k, v in config.items(section):
                task_options.ova_option.append('%s=%s' % (k, v))

        # as do factory-parameters
        section = 'factory-parameters'
        if config.has_section(section):
            task_options.factory_parameter = []
            for k, v in config.items(section):
                # We do this, rather than a dict, to match what argparse spits out
                task_options.factory_parameter.append((k, v))

    else:
        if len(args) < 5:
            parser.error("At least five arguments are required: a name, a version, "
                         "a build target, a URL to an install tree, and 1 or more architectures.")
    if not task_options.ksurl and not task_options.kickstart:
        parser.error('You must specify --kickstart')
    if not task_options.distro:
        parser.error(
            "You must specify --distro. Examples: Fedora-16, RHEL-6.4, SL-6.4 or CentOS-6.4")
    return _build_image_oz(options, task_options, session, args)


def _build_image(options, task_opts, session, args, img_type):
    """
    A private helper function that houses common CLI code for building
    images with chroot-based tools.
    """

    if img_type not in ('livecd', 'appliance', 'livemedia'):
        raise koji.GenericError('Unrecognized image type: %s' % img_type)
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
    target = args[2]
    tmp_target = session.getBuildTarget(target)
    if not tmp_target:
        raise koji.GenericError("No such build target: %s" % target)
    dest_tag = session.getTag(tmp_target['dest_tag'])
    if not dest_tag:
        raise koji.GenericError("No such destination tag: %s" % tmp_target['dest_tag_name'])

    # Set the architecture
    if img_type == 'livemedia':
        # livemedia accepts multiple arches
        arch = [koji.canonArch(a) for a in args[3].split(",")]
    else:
        arch = koji.canonArch(args[3])

    # Upload the KS file to the staging area.
    # If it's a URL, it's kojid's job to go get it when it does the checkout.
    ksfile = args[4]

    if not task_opts.ksurl:
        serverdir = unique_path('cli-' + img_type)
        session.uploadWrapper(ksfile, serverdir, callback=callback)
        ksfile = os.path.join(serverdir, os.path.basename(ksfile))
        print('')

    hub_opts = {}
    passthru_opts = [
        'format', 'install_tree_url', 'isoname', 'ksurl',
        'ksversion', 'release', 'repo', 'scratch', 'skip_tag',
        'specfile', 'vcpu', 'vmem', 'volid', 'optional_arches',
        'lorax_dir', 'lorax_url', 'nomacboot', 'ksrepo',
        'squashfs_only', 'compress_arg',
    ]
    for opt in passthru_opts:
        val = getattr(task_opts, opt, None)
        if val is not None:
            hub_opts[opt] = val

    if 'optional_arches' in hub_opts:
        hub_opts['optional_arches'] = hub_opts['optional_arches'].split(',')
    # finally, create the task.
    task_id = session.buildImage(args[0], args[1], arch, target, ksfile,
                                 img_type, opts=hub_opts, priority=priority)

    if not options.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if task_opts.wait or (task_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


def _build_image_oz(options, task_opts, session, args):
    """
    A private helper function that houses common CLI code for building
    images with Oz and ImageFactory
    """
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
    target = args[2]
    tmp_target = session.getBuildTarget(target)
    if not tmp_target:
        raise koji.GenericError("No such build target: %s" % target)
    dest_tag = session.getTag(tmp_target['dest_tag'])
    if not dest_tag:
        raise koji.GenericError("No such destination tag: %s" % tmp_target['dest_tag_name'])

    # Set the architectures
    arches = []
    for arch in args[4:]:
        arches.append(koji.canonArch(arch))

    # Upload the KS file to the staging area.
    # If it's a URL, it's kojid's job to go get it when it does the checkout.
    if not task_opts.ksurl:
        if not task_opts.scratch:
            # only scratch builds can omit ksurl
            raise koji.GenericError("Non-scratch builds must provide ksurl")
        ksfile = task_opts.kickstart
        serverdir = unique_path('cli-image')
        session.uploadWrapper(ksfile, serverdir, callback=callback)
        task_opts.kickstart = os.path.join('work', serverdir,
                                           os.path.basename(ksfile))
        print('')

    hub_opts = {}
    for opt in ('ksurl', 'ksversion', 'kickstart', 'scratch', 'repo',
                'release', 'skip_tag', 'specfile', 'distro', 'format',
                'disk_size', 'ova_option', 'factory_parameter',
                'optional_arches', 'noverifyssl'):
        val = getattr(task_opts, opt, None)
        if val is not None:
            hub_opts[opt] = val
    # finally, create the task.
    task_id = session.buildImageOz(args[0], args[1], arches, target, args[3],
                                   opts=hub_opts, priority=priority)

    if not options.quiet:
        print("Created task: %d" % task_id)
        print("Task info: %s/taskinfo?taskID=%s" % (options.weburl, task_id))
    if task_opts.wait or (task_opts.wait is None and not _running_in_bg()):
        session.logout()
        return watch_tasks(session, [task_id], quiet=options.quiet,
                           poll_interval=options.poll_interval, topurl=options.topurl)


