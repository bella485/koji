from __future__ import absolute_import, division

from optparse import OptionParser

from six.moves import filter, zip

import koji

from koji_cli.lib import (
    ensure_connection,
    get_usage_str,
    warn
)


def anon_handle_userinfo(goptions, session, args):
    """[admin] Show information about a user"""
    usage = "usage: %prog userinfo [options] <username> [<username> ...]"
    parser = OptionParser(usage=get_usage_str(usage))
    (options, args) = parser.parse_args(args)
    if len(args) < 1:
        parser.error("You must specify at least one username")

    ensure_connection(session, goptions)

    with session.multicall() as m:
        userinfos = [m.getUser(user) for user in args]
    user_infos = []
    for username, userinfo in zip(args, userinfos):
        if userinfo.result is None:
            warn("No such user: %s\n" % username)
            continue
        user_infos.append(userinfo.result)
    user_infos = list(filter(None, user_infos))

    calls = []
    with session.multicall() as m:
        for user in user_infos:
            results = []
            if not user:
                warn("No such user: %s\n" % user)
                continue
            results.append(m.getUserPerms(user['id']))
            results.append(m.listPackages(userID=user['id'], with_dups=True,
                                          queryOpts={'countOnly': True}))
            results.append(m.listTasks(opts={'owner': user['id'], 'parent': None},
                                       queryOpts={'countOnly': True}))
            results.append(m.listBuilds(userID=user['id'], queryOpts={'countOnly': True}))
            calls.append(results)

    for userinfo, (perms, pkgs, tasks, builds) in zip(user_infos, calls):
        print("User name: %s" % userinfo['name'])
        print("User ID: %d" % userinfo['id'])
        if 'krb_principals' in userinfo:
            print("krb principals:")
            for krb in userinfo['krb_principals']:
                print("  %s" % krb)
        if perms.result:
            print("Permissions:")
            for perm in perms.result:
                print("  %s" % perm)
        print("Status: %s" % koji.USER_STATUS[userinfo['status']])
        print("Usertype: %s" % koji.USERTYPES[userinfo['usertype']])
        print("Number of packages: %d" % pkgs.result)
        print("Number of tasks: %d" % tasks.result)
        print("Number of builds: %d" % builds.result)
        print('')
