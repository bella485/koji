from __future__ import absolute_import, division

import fnmatch
from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_lock_tag(goptions, session, args):
    "[admin] Lock a tag"
    usage = "usage: %prog lock-tag [options] <tag> [<tag> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--perm", help="Specify permission requirement")
    parser.add_option("--glob", action="store_true", help="Treat args as glob patterns")
    parser.add_option("--master", action="store_true", help="Lock the master lock")
    parser.add_option("-n", "--test", action="store_true", help="Test mode")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify a tag")
    activate_session(session, goptions)
    pdata = session.getAllPerms()
    perm_ids = dict([(p['name'], p['id']) for p in pdata])
    perm = options.perm
    if perm is None:
        perm = 'admin'
    perm_id = perm_ids[perm]
    if options.glob:
        selected = []
        for tag in session.listTags():
            for pattern in args:
                if fnmatch.fnmatch(tag['name'], pattern):
                    selected.append(tag)
                    break
        if not selected:
            print("No tags matched")
    else:
        selected = [session.getTag(name, strict=True) for name in args]
    for tag in selected:
        if options.master:
            # set the master lock
            if tag['locked']:
                print("Tag %s: master lock already set" % tag['name'])
                continue
            elif options.test:
                print("Would have set master lock for: %s" % tag['name'])
                continue
            session.editTag2(tag['id'], locked=True)
        else:
            if tag['perm_id'] == perm_id:
                print("Tag %s: %s permission already required" % (tag['name'], perm))
                continue
            elif options.test:
                print("Would have set permission requirement %s for tag %s" % (perm, tag['name']))
                continue
            session.editTag2(tag['id'], perm_id=perm_id)
