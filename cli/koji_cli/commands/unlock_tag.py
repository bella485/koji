from __future__ import absolute_import, division

import fnmatch
from optparse import OptionParser


from koji_cli.lib import (
    activate_session,
    get_usage_str
)


def handle_unlock_tag(goptions, session, args):
    "[admin] Unlock a tag"
    usage = "usage: %prog unlock-tag [options] <tag> [<tag> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--glob", action="store_true", help="Treat args as glob patterns")
    parser.add_option("-n", "--test", action="store_true", help="Test mode")
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("Please specify a tag")
    activate_session(session, goptions)
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
        selected = []
        for name in args:
            tag = session.getTag(name)
            if tag is None:
                parser.error("No such tag: %s" % name)
            selected.append(tag)
    for tag in selected:
        opts = {}
        if tag['locked']:
            opts['locked'] = False
        if tag['perm_id']:
            opts['perm'] = None
        if not opts:
            print("Tag %(name)s: not locked" % tag)
            continue
        if options.test:
            print("Tag %s: skipping changes: %r" % (tag['name'], opts))
        else:
            session.editTag2(tag['id'], **opts)
