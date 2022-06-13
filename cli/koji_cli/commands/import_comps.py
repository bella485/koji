from __future__ import absolute_import, division

from optparse import OptionParser

import six
import six.moves.xmlrpc_client


from koji_cli.lib import (
    activate_session,
    error,
    get_usage_str
)

try:
    import libcomps
except ImportError:  # pragma: no cover
    libcomps = None
    try:
        import yum.comps as yumcomps
    except ImportError:
        yumcomps = None


def handle_import_comps(goptions, session, args):
    "Import group/package information from a comps file"
    usage = "usage: %prog import-comps [options] <file> <tag>"
    parser = OptionParser(usage=get_usage_str(usage))
    parser.add_option("--force", action="store_true", help="force import")
    (local_options, args) = parser.parse_args(args)
    if len(args) != 2:
        parser.error("Incorrect number of arguments")
    activate_session(session, goptions)
    # check if the tag exists
    dsttag = session.getTag(args[1])
    if dsttag is None:
        error("No such tag: %s" % args[1])
    if libcomps is not None:
        _import_comps(session, args[0], args[1], local_options)
    elif yumcomps is not None:
        _import_comps_alt(session, args[0], args[1], local_options)
    else:
        error("comps module not available")


def _import_comps(session, filename, tag, options):
    """Import comps data using libcomps module"""
    comps = libcomps.Comps()
    comps.fromxml_f(filename)
    force = options.force
    ptypes = {
        libcomps.PACKAGE_TYPE_DEFAULT: 'default',
        libcomps.PACKAGE_TYPE_OPTIONAL: 'optional',
        libcomps.PACKAGE_TYPE_CONDITIONAL: 'conditional',
        libcomps.PACKAGE_TYPE_MANDATORY: 'mandatory',
        libcomps.PACKAGE_TYPE_UNKNOWN: 'unknown',
    }
    for group in comps.groups:
        print("Group: %s (%s)" % (group.id, group.name))
        session.groupListAdd(
            tag, group.id, force=force, display_name=group.name,
            is_default=bool(group.default),
            uservisible=bool(group.uservisible),
            description=group.desc,
            langonly=group.lang_only,
            biarchonly=bool(group.biarchonly))
        for pkg in group.packages:
            pkgopts = {'type': ptypes[pkg.type],
                       'basearchonly': bool(pkg.basearchonly),
                       }
            if pkg.type == libcomps.PACKAGE_TYPE_CONDITIONAL:
                pkgopts['requires'] = pkg.requires
            for k in pkgopts.keys():
                if six.PY2 and isinstance(pkgopts[k], unicode):  # noqa: F821
                    pkgopts[k] = str(pkgopts[k])
            s_opts = ', '.join(["'%s': %r" % (k, pkgopts[k]) for k in sorted(pkgopts.keys())])
            print("  Package: %s: {%s}" % (pkg.name, s_opts))
            session.groupPackageListAdd(tag, group.id, pkg.name, force=force, **pkgopts)
        # libcomps does not support group dependencies
        # libcomps does not support metapkgs


def _import_comps_alt(session, filename, tag, options):  # no cover 3.x
    """Import comps data using yum.comps module"""
    print('WARN: yum.comps does not support the biarchonly of group and basearchonly of package')
    comps = yumcomps.Comps()
    comps.add(filename)
    force = options.force
    for group in comps.groups:
        print("Group: %(groupid)s (%(name)s)" % vars(group))
        session.groupListAdd(tag, group.groupid, force=force, display_name=group.name,
                             is_default=bool(group.default),
                             uservisible=bool(group.user_visible),
                             description=group.description,
                             langonly=group.langonly)
        # yum.comps does not support the biarchonly field
        for ptype, pdata in [('mandatory', group.mandatory_packages),
                             ('default', group.default_packages),
                             ('optional', group.optional_packages),
                             ('conditional', group.conditional_packages)]:
            for pkg in pdata:
                # yum.comps does not support basearchonly
                pkgopts = {'type': ptype}
                if ptype == 'conditional':
                    pkgopts['requires'] = pdata[pkg]
                for k in pkgopts.keys():
                    if six.PY2 and isinstance(pkgopts[k], unicode):  # noqa: F821
                        pkgopts[k] = str(pkgopts[k])
                s_opts = ', '.join(["'%s': %r" % (k, pkgopts[k]) for k in sorted(pkgopts.keys())])
                print("  Package: %s: {%s}" % (pkg, s_opts))
                session.groupPackageListAdd(tag, group.groupid, pkg, force=force, **pkgopts)
        # yum.comps does not support group dependencies
        # yum.comps does not support metapkgs
