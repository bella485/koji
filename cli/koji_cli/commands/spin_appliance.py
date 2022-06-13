from __future__ import absolute_import, division

from optparse import OptionParser


from koji_cli.lib import (
    get_usage_str,
    build_image,
)


def handle_spin_appliance(options, session, args):
    """[build] Create an appliance given a kickstart file"""

    # Usage & option parsing
    usage = "usage: %prog spin-appliance [options] <name> <version> <target> <arch> " \
            "<kickstart-file>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--wait", action="store_true",
                      help="Wait on the appliance creation, even if running in the background")
    parser.add_option("--nowait", action="store_false", dest="wait",
                      help="Don't wait on appliance creation")
    parser.add_option("--noprogress", action="store_true",
                      help="Do not display progress of the upload")
    parser.add_option("--background", action="store_true",
                      help="Run the appliance creation task at a lower priority")
    parser.add_option("--ksurl", metavar="SCMURL",
                      help="The URL to the SCM containing the kickstart file")
    parser.add_option("--ksversion", metavar="VERSION",
                      help="The syntax version used in the kickstart file")
    parser.add_option("--scratch", action="store_true", help="Create a scratch appliance")
    parser.add_option("--repo", action="append",
                      help="Specify a repo that will override the repo used to install "
                           "RPMs in the appliance. May be used multiple times. The "
                           "build tag repo associated with the target is the default.")
    parser.add_option("--release", help="Forcibly set the release field")
    parser.add_option("--specfile", metavar="URL",
                      help="SCM URL to spec file fragment to use to generate wrapper RPMs")
    parser.add_option("--skip-tag", action="store_true", help="Do not attempt to tag package")
    parser.add_option("--vmem", metavar="VMEM", default=None,
                      help="Set the amount of virtual memory in the appliance in MB, "
                           "default is 512")
    parser.add_option("--vcpu", metavar="VCPU", default=None,
                      help="Set the number of virtual cpus in the appliance, default is 1")
    parser.add_option("--format", metavar="DISK_FORMAT", default='raw',
                      help="Disk format, default is raw. Other options are qcow, qcow2, and vmx.")

    (task_options, args) = parser.parse_args(args)

    # Make sure the target and kickstart is specified.
    print('spin-appliance is deprecated and will be replaced with image-build')
    if len(args) != 5:
        parser.error("Five arguments are required: a name, a version, an architecture, "
                     "a build target, and a relative path to a kickstart file.")
    return build_image(options, task_options, session, args, 'appliance')
