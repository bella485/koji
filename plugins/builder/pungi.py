# kojid plugin

from __future__ import absolute_import
import six
from six.moves import shlex_quote
import os

import koji
import koji.tasks


__all__ = ('PungiBuildinstallTask',)


class PungiBuildinstallTask(koji.tasks.BaseTaskHandler):

    Methods = ['pungi_buildinstall']

    _taskWeight = 0.5

    def __init__(self, *args, **kwargs):
        self.allowed_lorax_args = set([
            "product",
            "version",
            "release",
            "sources",
            "variant",
            "bugurl",
            "nomacboot",
            "noupgrade",
            "isfinal",
            "buildarch",
            "volid",
            "installpkgs",
            "add-template",
            "add-arch-template",
            "add-template-var",
            "add-arch-template-var",
            "rootfs-size",
            "dracut-args"
        ])
        return super(PungiBuildinstallTask, self).__init__(*args, **kwargs)

    def handler(self, tag, arch, packages=[], mounts=[], weight=None, lorax_args=None,
                chown_uid=None):
        if weight is not None:
            weight = max(weight, 0.5)
            self.session.host.setTaskWeight(self.id, weight)

        if lorax_args is None:
            lorax_args = {}

        if "outputdir" in lorax_args:
            output_dir = lorax_args["outputdir"]
            del lorax_args["outputdir"]
        else:
            output_dir = self.workdir

        if "lorax" not in packages:
            packages.append("lorax")

        # Raise an exception if not allowed argument is used.
        not_allowed_args = set(lorax_args.keys()) - self.allowed_lorax_args
        if not_allowed_args:
            args = ', '.join(str(x) for x in not_allowed_args)
            raise koji.GenericError("Not allowed lorax arguments found: %s." % args)

        # Generate the lorax command with lorax_args.
        lorax_cmd = "lorax"
        for opt, arg in lorax_args.items():
            if opt == "sources":
                for source in arg:
                    if "://" not in source:
                        source = "file://%s" % source
                    quoted_source = shlex_quote(source)
                    lorax_cmd +=" --source=%s" % quoted_source
            elif opt == "dracut-args":
                for dracut_arg in arg:
                    quoted_arg = shlex_quote(dracut_arg)
                    lorax_cmd += " --dracut-arg=%s" % quoted_arg
            elif isinstance(arg, list):
                for lorax_arg in arg:
                    quoted_arg = shlex_quote(lorax_arg)
                    lorax_cmd += " --%s=%s" % (opt, quoted_arg)
            elif isinstance(arg, six.string_types):
                quoted_arg = shlex_quote(arg)
                lorax_cmd += " --%s=%s" % (opt, quoted_arg)
            elif arg:
                lorax_cmd += " --%s" % opt

        if os.path.exists(output_dir):
            raise koji.GenericError('The "outputdir" "%s" already exists.' % output_dir)

        # Create log directory and add --logfile.
        logdir = os.path.join(output_dir, "logs")
        os.makedirs(logdir)
        logfile = os.path.join(logdir, "lorax.log")
        lorax_cmd += " --logfile=%s" % logfile

        # Set the output directory and add it to lorax_cmd. This directory
        # must not exist, otherwise the Lorax command fails, so we won't
        # create it.
        result_dir = os.path.join(output_dir, "results")
        lorax_cmd += " %s" % result_dir

        if chown_uid:
            # Store the exit code of "lorax" command.
            lorax_cmd += "; ret=$?;"
            # Run chmod/chown to make the lorax output readible for requester.
            lorax_cmd += " chmod -R a+r %s" % shlex_quote(output_dir)
            lorax_cmd += " && chown -R %s %s" % (
                shlex_quote(str(chown_uid)), shlex_quote(output_dir))
            # Exit with the original lorax exit code.
            lorax_cmd += "; exit $ret"

        # Execute runroot subtask.
        kwargs = {
            "mounts": mounts,
            "packages": packages,
        }
        task_id = self.session.host.subtask(
            method='runroot', arglist=[tag, arch, lorax_cmd], parent=self.id, kwargs=kwargs)

        # In case the runroot task fails, this raises an exception.
        self.wait(task_id)

        return "Completed successfully"
