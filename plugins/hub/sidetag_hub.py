# Copyright © 2019 Red Hat, Inc.
#
# SPDX-License-Identifier: GPL-2.0-or-later
import sys

import koji
from koji.context import context
from koji.plugin import callback, export
sys.path.insert(0, "/usr/share/koji-hub/")
from kojihub import (  # noqa: F402
    QueryProcessor,
    _create_build_target,
    _create_tag,
    _delete_build_target,
    _delete_tag,
    assert_policy,
    get_build_target,
    get_tag,
    get_user,
    nextval
)

CONFIG_FILE = "/etc/koji-hub/plugins/sidetag.conf"
CONFIG = None


@export
def createSideTag(basetag, debuginfo=False):
    """Create a side tag.

    :param basetag: name or ID of base tag
    :type basetag: str or int

    :param debuginfo: should buildroot repos contain debuginfo?
    :type debuginfo: bool
    """

    # Any logged-in user is able to request creation of side tags,
    # as long the request meets the policy.
    context.session.assertLogin()
    user = get_user(context.session.user_id, strict=True)

    basetag = get_tag(basetag, strict=True)

    query = QueryProcessor(
        tables=["tag_extra"],
        clauses=["key='sidetag_user_id'", "value=%(user_id)s", "active IS TRUE"],
        columns=["COUNT(*)"],
        aliases=["user_tags"],
        values={"user_id": str(user["id"])},
    )
    user_tags = query.executeOne()
    if user_tags is None:
        # should not ever happen
        raise koji.GenericError("Unknown db error")

    # Policy is a very flexible mechanism, that can restrict for which
    # tags sidetags can be created, or which users can create sidetags etc.
    assert_policy(
        "sidetag", {"tag": basetag["id"], "number_of_tags": user_tags["user_tags"]}
    )

    # ugly, it will waste one number in tag_id_seq, but result will match with
    # id assigned by _create_tag
    tag_id = nextval("tag_id_seq") + 1
    sidetag_name = "%s-side-%s" % (basetag["name"], tag_id)
    extra = {
        "sidetag": True,
        "sidetag_user": user["name"],
        "sidetag_user_id": user["id"],
    }
    if debuginfo:
        extra['with_debuginfo'] = True
    sidetag_id = _create_tag(
        sidetag_name,
        parent=basetag["id"],
        arches=basetag["arches"],
        extra=extra,
    )
    _create_build_target(sidetag_name, sidetag_id, sidetag_id)

    return {"name": sidetag_name, "id": sidetag_id}


@export
def removeSideTag(sidetag):
    """Remove a side tag

    :param sidetag: id or name of sidetag
    :type sidetag: int or str
    """
    context.session.assertLogin()
    user = get_user(context.session.user_id, strict=True)
    sidetag = get_tag(sidetag, strict=True)

    # sanity/access
    if not sidetag["extra"].get("sidetag"):
        raise koji.GenericError("Not a sidetag: %(name)s" % sidetag)
    if sidetag["extra"].get("sidetag_user_id") != user["id"]:
        if not context.session.hasPerm("admin"):
            raise koji.ActionNotAllowed("This is not your sidetag")
    _remove_sidetag(sidetag)


def _remove_sidetag(sidetag):
    # check target
    target = get_build_target(sidetag["name"])
    if not target:
        raise koji.GenericError("Target is missing for sidetag")
    if target["build_tag"] != sidetag["id"] or target["dest_tag"] != sidetag["id"]:
        raise koji.GenericError("Target does not match sidetag")

    _delete_build_target(target["id"])
    _delete_tag(sidetag["id"])


@export
def listSideTags(basetag=None, user=None, queryOpts=None):
    """List all sidetags with additional filters

    :param basetag: filter by basteag id or name
    :type basetag: int or str
    :param user: filter by userid or username
    :type user: int or str
    :param queryOpts: additional query options
                      {countOnly, order, offset, limit}
    :type queryOpts: dict

    :returns: list of dicts: id, name, user_id, user_name
    """
    # te1.sidetag
    # te2.user_id
    # te3.basetag
    if user is not None:
        user_id = str(get_user(user, strict=True)["id"])
    else:
        user_id = None
    if basetag is not None:
        basetag_id = get_tag(basetag, strict=True)["id"]
    else:
        basetag_id = None

    joins = [
        "LEFT JOIN tag_extra AS te1 ON tag.id = te1.tag_id",
        "LEFT JOIN tag_extra AS te2 ON tag.id = te2.tag_id",
        "LEFT JOIN users ON CAST(te2.value AS INTEGER) = users.id",
    ]
    clauses = [
        "te1.active IS TRUE",
        "te1.key = 'sidetag'",
        "te1.value = 'true'",
        "te2.active IS TRUE",
        "te2.key = 'sidetag_user_id'"
    ]
    if user_id:
        clauses.append("te2.value = %(user_id)s")
    if basetag_id:
        joins.append("LEFT JOIN tag_inheritance ON tag.id = tag_inheritance.tag_id")
        clauses.extend(
            [
                "tag_inheritance.active IS TRUE",
                "tag_inheritance.parent_id = %(basetag_id)s",
            ]
        )

    query = QueryProcessor(
        tables=["tag"],
        clauses=clauses,
        columns=["tag.id", "tag.name", "te2.value", "users.name"],
        aliases=["id", "name", "user_id", "user_name"],
        joins=joins,
        values={"basetag_id": basetag_id, "user_id": user_id},
        opts=queryOpts,
    )
    return query.execute()


def handle_sidetag_untag(cbtype, *args, **kws):
    """Remove a side tag when its last build is untagged

    Note, that this is triggered only in case, that some build exists. For
    never used tags, some other policy must be applied. Same holds for users
    which don't untag their builds.
    """
    if "tag" not in kws:
        # shouldn't happen, but...
        return
    tag = get_tag(kws["tag"]["id"], strict=False)
    if not tag:
        # also shouldn't happen, but just in case
        return
    if not tag["extra"].get("sidetag"):
        # not a side tag
        return
    # is the tag now empty?
    query = QueryProcessor(
        tables=["tag_listing"],
        clauses=["tag_id = %(tag_id)s", "active IS TRUE"],
        values={"tag_id": tag["id"]},
        opts={"countOnly": True},
    )
    if query.execute():
        return
    # looks like we've just untagged the last build from a side tag
    try:
        # XXX: are we double updating tag_listing?
        _remove_sidetag(tag)
    except koji.GenericError:
        pass


# read config and register
if not CONFIG:
    CONFIG = koji.read_config_files(CONFIG_FILE)
    if CONFIG.has_option("sidetag", "remove_empty") and CONFIG.getboolean(
        "sidetag", "remove_empty"
    ):
        handle_sidetag_untag = callback("postUntag")(handle_sidetag_untag)
