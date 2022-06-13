from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    get_usage_str,
    _build_image,
)


def handle_spin_livemedia(options, session, args):
    """[build] Create a livemedia image given a kickstart file"""

    # Usage & option parsing.
    usage = "usage: %prog spin-livemedia [options] <name> <version> <target> <arch> " \
            "<kickstart-file>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--wait", action="store_true",
                      help="Wait on the livemedia creation, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Don't wait on livemedia creation")
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")
    parser.add_option("--background", action="store_true",
                      help="Run the livemedia creation task at a lower priority")
    parser.add_option("--ksurl", metavar="SCMURL",
                      help="The URL to the SCM containing the kickstart file")
    parser.add_option("--install-tree-url", metavar="URL",
                      help="Provide the URL for the install tree")
    parser.add_option("--ksversion", metavar="VERSION",
                      help="The syntax version used in the kickstart file")
    parser.add_option("--scratch", action="store_true",
                      help="Create a scratch LiveMedia image")
    parser.add_option("--repo", action="append",
                      help="Specify a repo that will override the repo used to install "
                           "RPMs in the LiveMedia. May be used multiple times. The "
                           "build tag repo associated with the target is the default.")
    parser.add_option("--release", help="Forcibly set the release field")
    parser.add_option("--volid", help="Set the volume id")
    parser.add_option("--specfile", metavar="URL",
                      help="SCM URL to spec file fragment to use to generate wrapper RPMs")
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag package")
    parser.add_option("--can-fail", action="store", dest="optional_arches",
                      metavar="ARCH1,ARCH2,...", default="",
                      help="List of archs which are not blocking for build (separated by commas.")
    parser.add_option('--lorax_dir', metavar='DIR',
                      help='The relative path to the lorax templates '
                           'directory within the checkout of "lorax_url".')
    parser.add_option('--lorax_url', metavar='URL',
                      help='The URL to the SCM containing any custom lorax templates that are '
                           'to be used to override the default templates.')
    parser.add_option('--nomacboot', action="store_true",
                      help="Pass the nomacboot option to livemedia-creator")
    parser.add_option('--ksrepo', action="store_true",
                      help="Do not overwrite repos in the kickstart")
    parser.add_option('--squashfs-only', action="store_true",
                      help="Use a plain squashfs filesystem.")
    parser.add_option('--compress-arg', action="append", default=[], metavar="ARG OPT",
                      help="List of compressions.")
    (task_options, args) = parser.parse_args(args)

    # Make sure the target and kickstart is specified.
    if len(args) != 5:
        parser.error("Five arguments are required: a name, a version, a build target, "
                     "an architecture, and a relative path to a kickstart file.")
    if task_options.lorax_url is not None and task_options.lorax_dir is None:
        parser.error('The "--lorax_url" option requires that "--lorax_dir" also be used.')
    if task_options.volid is not None and len(task_options.volid) > 32:
        parser.error('Volume ID has a maximum length of 32 characters')
    return _build_image(options, task_options, session, args, 'livemedia')
