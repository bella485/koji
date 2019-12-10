#koji hub plugin
# There is a kojid plugin that goes with this hub plugin. The kojid builder
# plugin has a config file.  This hub plugin has no config file.


from __future__ import absolute_import
from koji.context import context
from koji.plugin import export
import koji
import sys

#XXX - have to import kojihub for make_task
sys.path.insert(0, '/usr/share/koji-hub/')
import kojihub

__all__ = ('pungi_buildinstall',)


@export
def pungi_buildinstall(tag, arch, channel=None, **opts):
    """ Create a pungi_buildinstall task """
    context.session.assertPerm('pungi_buildinstall')
    taskopts = {
        'priority': 15,
        'arch': arch,
    }

    taskopts['channel'] = channel or 'runroot'

    args = koji.encode_args(tag, arch, **opts)
    return kojihub.make_task('pungi_buildinstall', args, **taskopts)

