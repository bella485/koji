from __future__ import absolute_import, division

from optparse import OptionParser

import koji

from koji_cli.lib import (
    ensure_connection,
    error,
    get_usage_str,
    warn
)


def anon_handle_mock_config(goptions, session, args):
    "[info] Create a mock config"
    usage = "usage: %prog mock-config [options]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("-a", "--arch", help="Specify the arch")
    parser.add_option("-n", "--name", help="Specify the name for the buildroot")
    parser.add_option("--tag", help="Create a mock config for a tag")
    parser.add_option("--target", help="Create a mock config for a build target")
    parser.add_option("--task", help="Duplicate the mock config of a previous task")
    parser.add_option("--latest", action="store_true", help="use the latest redirect url")
    parser.add_option("--buildroot",
                      help="Duplicate the mock config for the specified buildroot id")
    parser.add_option("--mockdir", default="/var/lib/mock", metavar="DIR", help="Specify mockdir")
    parser.add_option("--topdir", metavar="DIR",
                      help="Specify topdir, topdir tops the topurl")
    parser.add_option("--topurl", metavar="URL",
                      help="URL under which Koji files are accessible, "
                           "when topdir is specified, topdir tops the topurl")
    parser.add_option("--distribution", default="Koji Testing",
                      help="Change the distribution macro")
    parser.add_option("--yum-proxy", help="Specify a yum proxy")
    parser.add_option("-o", metavar="FILE", dest="ofile", help="Output to a file")
    (options, args) = parser.parse_args(args)
    ensure_connection(session, goptions)
    if args:
        # for historical reasons, we also accept buildroot name as first arg
        if not options.name:
            options.name = args[0]
        else:
            parser.error("Name already specified via option")
    arch = None
    opts = {}
    for k in ('topdir', 'topurl', 'distribution', 'mockdir', 'yum_proxy'):
        if hasattr(options, k):
            if getattr(options, k) is not None:
                opts[k] = getattr(options, k)
    if opts.get('topdir') and opts.get('topurl'):
        del opts['topurl']
    if not opts.get('topdir') and not opts.get('topurl'):
        opts['topurl'] = goptions.topurl
    if options.buildroot:
        try:
            br_id = int(options.buildroot)
        except ValueError:
            parser.error("Buildroot id must be an integer")
        brootinfo = session.getBuildroot(br_id)
        if brootinfo is None:
            error("No such buildroot: %r" % br_id)
        if options.latest:
            opts['repoid'] = 'latest'
        else:
            opts['repoid'] = brootinfo['repo_id']
        opts['tag_name'] = brootinfo['tag_name']
        arch = brootinfo['arch']
    elif options.task:
        try:
            task_id = int(options.task)
        except ValueError:
            parser.error("Task id must be an integer")
        broots = session.listBuildroots(taskID=task_id)
        if not broots:
            error("No buildroots for task %s (or no such task)" % options.task)
        if len(broots) > 1:
            print("Multiple buildroots found: %s" % [br['id'] for br in broots])
        brootinfo = broots[-1]
        if options.latest:
            opts['repoid'] = 'latest'
        else:
            opts['repoid'] = brootinfo['repo_id']
        opts['tag_name'] = brootinfo['tag_name']
        arch = brootinfo['arch']
        if not options.name:
            options.name = "%s-task_%i" % (opts['tag_name'], task_id)
    elif options.tag:
        if not options.arch:
            error("Please specify an arch")
        tag = session.getTag(options.tag)
        if not tag:
            parser.error("No such tag: %s" % options.tag)
        arch = options.arch
        config = session.getBuildConfig(tag['id'])
        if not config:
            error("Could not get config info for tag: %(name)s" % tag)
        opts['tag_name'] = tag['name']
        if options.latest:
            opts['repoid'] = 'latest'
        else:
            repo = session.getRepo(config['id'])
            if not repo:
                error("Could not get a repo for tag: %(name)s" % tag)
            opts['repoid'] = repo['id']
    elif options.target:
        if not options.arch:
            error("Please specify an arch")
        arch = options.arch
        target = session.getBuildTarget(options.target)
        if not target:
            parser.error("No such build target: %s" % options.target)
        opts['tag_name'] = target['build_tag_name']
        if options.latest:
            opts['repoid'] = 'latest'
        else:
            repo = session.getRepo(target['build_tag'])
            if not repo:
                error("Could not get a repo for tag: %s" % opts['tag_name'])
            opts['repoid'] = repo['id']
    else:
        parser.error("Please specify one of: --tag, --target, --task, --buildroot")
    if options.name:
        name = options.name
    else:
        name = "%(tag_name)s-repo_%(repoid)s" % opts

    event = None
    if opts['repoid'] != 'latest':
        event = session.repoInfo(opts['repoid'])['create_event']
    buildcfg = session.getBuildConfig(opts['tag_name'], event=event)
    if arch:
        if not buildcfg['arches']:
            warn("Tag %s has an empty arch list" % opts['tag_name'])
        elif arch not in buildcfg['arches']:
            warn('%s is not in the list of tag arches' % arch)
        if 'mock.forcearch' in buildcfg['extra']:
            if bool(buildcfg['extra']['mock.forcearch']):
                opts['forcearch'] = arch
    if 'mock.package_manager' in buildcfg['extra']:
        opts['package_manager'] = buildcfg['extra']['mock.package_manager']
    if 'mock.yum.module_hotfixes' in buildcfg['extra']:
        opts['module_hotfixes'] = buildcfg['extra']['mock.yum.module_hotfixes']
    if 'mock.yum.best' in buildcfg['extra']:
        opts['yum_best'] = int(buildcfg['extra']['mock.yum.best'])
    if 'mock.bootstrap_image' in buildcfg['extra']:
        opts['use_bootstrap_image'] = True
        opts['bootstrap_image'] = buildcfg['extra']['mock.bootstrap_image']
    if 'mock.use_bootstrap' in buildcfg['extra']:
        opts['use_bootstrap'] = buildcfg['extra']['mock.use_bootstrap']
    if 'mock.module_setup_commands' in buildcfg['extra']:
        opts['module_setup_commands'] = buildcfg['extra']['mock.module_setup_commands']
    if 'mock.releasever' in buildcfg['extra']:
        opts['releasever'] = buildcfg['extra']['mock.releasever']
    opts['tag_macros'] = {}
    for key in buildcfg['extra']:
        if key.startswith('rpm.macro.'):
            macro = '%' + key[10:]
            opts['tag_macros'][macro] = buildcfg['extra'][key]
    output = koji.genMockConfig(name, arch, **opts)
    if options.ofile:
        with open(options.ofile, 'wt') as fo:
            fo.write(output)
    else:
        print(output)
