'''
 This plugin will tag builds of a specific target into multiple tags.

Example Config File:
$ cat /etc/koji-hub/plugins/target_multi_tag.conf
[koji_target_name1]
extra_tags = tagname2, tagname3, tagname4

[koji_target_two]
extra_tags = tagname6
'''
#
# Authors:
#     Pat Riehecky <riehecky@fnal.gov>

from __future__ import absolute_import
import logging

import six.moves.configparser

from koji import BUILD_STATES
from koji.context import context
from koji.plugin import callback

CONFIG_FILE = '/etc/koji-hub/plugins/target_multi_tag.conf'
CONFIG = None

@callback('postBuildStateChange')
def tag_target_into_multiple(cbtype, *args, **kws):
    """Tag a completed build into other non-default tags automatically"""

    # If the build wasn't successful do nothing
    if BUILD_STATES[kws['new']] != 'COMPLETE':
        return True

    log = logging.getLogger('koji.plugin.target_multi_tag')

    # read in config once and store it
    global CONFIG
    if not CONFIG:
        conf = six.moves.configparser.SafeConfigParser()
        log.info('reading in config file: %s', CONFIG_FILE)
        with open(CONFIG_FILE) as conffile:
            conf.readfp(conffile)
        CONFIG = conf

    # get build information
    task_id = kws['info']['task_id']
    build_id = kws['info']['build_id']
    task_info = context.handlers.call('getTaskInfo', task_id, request=True)
    target = task_info['request'][1]
    opts = task_info['request'][2]

    #  scratch or skip-tag builds don't tag already
    if 'skip_tag' in opts:
        if opts['skip_tag']:
            return True
    if 'scratch' in opts:
        if opts['scratch']:
            return True

    if target not in CONFIG.sections():
        log.debug('target:%s not in config file:%s', target, CONFIG_FILE)
        return True

    if CONFIG.has_option(target, 'extra_tags'):
        # csv with optional spaces
        extra_tags = CONFIG.get(target, 'extra_tags').replace(' ', '').split(',')
    else:
        log.warning('target:%s in config file:%s has no "extra_tags"', target, CONFIG_FILE)
        return True

    log.debug("target:%s in config file:%s has extra_tags:%s", target, CONFIG_FILE, extra_tags)

    current_tags = []
    for tag in context.handlers.call('listTags', build=build_id):
        current_tags.append(tag['name'])

    target_info = context.handlers.call('getBuildTarget', info=target)
    current_tags.append(target_info['dest_tag_name'])

    for tagname in extra_tags:
        if tagname not in current_tags:
            log.debug("target:%s for %s adding tag %s", target, kws['info']['nvr'], tagname)
            context.handlers.call('host.subtask',
                                  method='tagBuild',
                                  arglist=[tagname, kws['info']['nvr']],
                                  parent=task_id)
        else:
            log.debug("target:%s for %s already in tag %s", target, kws['info']['nvr'], tagname)
