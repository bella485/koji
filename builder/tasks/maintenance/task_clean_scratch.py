import errno
import koji
import time
import os
from koji.tasks import BaseTaskHandler
from koji.util import multi_fnmatch, rmtree

"""
# we completely remove those that are old enough
# scratch directories are /mnt/brew/scratch/$username/task_$taskid/
# note that $username might contain a slash (e.g. host principals)
find /mnt/brew/scratch/ -mindepth 2 -type d -name 'task_*' -prune -mtime +21 -exec rm -rf {} \;

# For content besides srpms/logs/poms we prune much more aggressively
# note that this step normally alters the mtime of the task dir, effectively
# adding to the above retention window.
for taskdir in $(find /mnt/brew/scratch/ -mindepth 2 -type d -name 'task_*' -prune -mtime +14)
do
    find "$taskdir" -type f \! -name '*.src.rpm' \! -name '*.log' \! -name '*.pom' -delete
done

find /mnt/brew/scratch/ -maxdepth 1 -type d -mtime +1 -empty -delete
"""


class CleanScratchTask(BaseTaskHandler):
    HandlerType = 'maintenance'
    Methods = ['maintCleanScratch']
    _taskWeight = 0.2

    def handler(self):
        scratch_dir = koji.pathinfo.scratch()
        if not os.access(scratch_dir, os.R_OK | os.W_OK | os.X_OK):
            raise koji.ConfigurationError(
                f"This builder doesn't have RW access to scratch dir {scratch_dir}")

        # we completely remove those that are old enough
        # scratch directories are /mnt/brew/scratch/$username/task_$taskid/
        # note that $username might contain a slash (e.g. host principals)
        now = time.time()
        prune_limit = now - 21 * 24 * 60 * 60
        partial_prune_limit = now - 14 * 24 * 60 * 60
        partial_prune_list = ['.src.rpm', '.log', '.pom']
        empty_userdir_limit = 1 * 24 * 60 * 60

        for userdir in os.listdir(scratch_dir):
            fuserdir = os.path.join(scratch_dir, userdir)
            empty_userdir = True
            for taskdir in os.listdir(fuserdir):
                empty_userdir = False
                if not taskdir.startswith('task_'):
                    # skip anything not produced by kojid
                    pass
                ftaskdir = os.path.join(fuserdir, taskdir)
                mtime = os.path.getmtime(ftaskdir)
                if mtime < prune_limit:
                    # delete old task directories
                    rmtree(ftaskdir)
                elif mtime < partial_prune_limit:
                    # delete most of the content except srpms, logs, ...
                    for root, _, files in os.walk(ftaskdir):
                        files = [f for f in files if multi_fnmatch(f, partial_prune_list)]
                        for f in files:
                            fpath = os.path.join(root, f)
                            if os.path.getmtime(fpath) < partial_prune_limit:
                                os.unlink(fpath)
            # remove userdir if it is empty for some time
            if empty_userdir and os.path.getmtime(fuserdir) < empty_userdir_limit:
                try:
                    os.rmdir(fuserdir)
                except OSError as ex:
                    # there could be a race condition that some scratch build is being created
                    if ex.errno != errno.ENOTEMPTY:
                        raise
