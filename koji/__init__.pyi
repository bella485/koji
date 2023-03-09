# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this library; if not, see <http://www.gnu.org/licenses/>.


"""
Koji - type stubs

Typing annotations stub for the parts of koji used by koji smoky
dingo. In particular there are annotations for the virtual XMLRPC
methods on the ClientSession class which should help check that the
calls are being used correctly.

:author: Christopher O'Brien <obriencj@gmail.com>
:license: GPL v3
"""

import io
from types import ModuleType
import requests
import xml.sax.handler
from configparser import ConfigParser, RawConfigParser
from optparse import Values
from datetime import datetime
from typing import (
    Any, Callable, Collection, Dict, Generator, IO, Iterable, List,
    Optional, Self, Sequence, Set, Tuple, TypeAlias, TypedDict,
    Union, overload
)
from xmlrpc.client import DateTime


__version__: str
__version_info__: Tuple[int]

_StrDict: TypeAlias = Dict[str, Any]
_Options: TypeAlias = Optional[_StrDict]

# simple aliases
_ArchesType: TypeAlias = Union[str, Iterable[str]]
_Timestamp: TypeAlias = Union[str, float]


# types

class _ArchiveInfo(TypedDict):
    """
    Data representing a koji archive. These are typically obtained via
    the ``getArchive`` or ``listArchives`` XMLRPC calls
    """

    btype: str
    """ Name of this archive's koji BType. eg. 'maven' or 'image' """

    btype_id: int
    """ ID of this archive's koji BType """

    build_id: int
    """ ID of koji build owning this archive """

    buildroot_id: int
    """ ID of the koji buildroot used to produce this archive """

    checksum: str
    """ hex representation of the checksum for this archive """

    checksum_type: int
    """ type of cryptographic checksum used in the `checksum` field """

    extra: dict
    """ additional metadata provided by content generators """

    filename: str
    """ base filename for this archive """

    id: int
    """ internal ID """

    metadata_only: bool

    size: int
    """ filesize in bytes """

    type_description: str
    """ this archive's type description """

    type_extensions: str
    """ space-delimited extensions shared by this archive's type """

    type_id: int
    """ ID of the archive's type """

    type_name: str
    """ name of the archive's type. eg. 'zip' or 'pom' """

    artifact_id: str
    """ Only present on maven archives. The maven artifact's name """

    group_id: str
    """ Only present on maven archives. The maven artifact's group """

    version: str
    """ Only present on maven archives. The maven artifact's version """

    platforms: List[str]
    """ Only present on Windows archives """

    relpath: str
    """ Only present on Windows archives """

    flags: str
    """ Only present on Windows archives """

    arch: str
    """ Only present on Image archives """


_ArchiveInfos = Collection[_ArchiveInfo]
""" An Collection of _ArchiveInfo dicts """


class _ArchiveTypeInfo(TypedDict):

    description: str
    """ short title of the type """

    extensions: str
    """ space separated extensions for this type """

    id: int
    """ the internal ID of the archive type """

    name: str
    """ the name of the archive type """


class _BTypeInfo(TypedDict):
    id: int
    """ the internal ID of the btype """

    name: str
    """ the name of the btype """


class _BuildInfo(TypedDict):
    """
    Data representing a koji build. These are typically obtained via
    the ``getBuild`` XMLRPC call.
    """

    build_id: int
    """ The internal ID for the build record """

    cg_id: int
    """ The ID of the content generator which has reserved or produced
    this build """

    cg_name: str
    """ The name of the content generator which has reserved or produced
    this build """

    completion_time: str
    """ ISO-8601 formatted UTC datetime stamp indicating when this build
    was completed """

    completion_ts: float
    """ UTC timestamp indicating when this build was completed """

    creation_event_id: int
    """ koji event ID at the creation of this build record """

    creation_time: str
    """ ISO-8601 formatted UTC datetime stamp indicating when this build
    record was created """

    creation_ts: float
    """ UTC timestamp indicating when this build record was created """

    epoch: str
    """ epoch of this build, or None if unspecified. This field is
    typically only used for RPM builds which have specified an epoch
    in their spec. """

    extra: dict
    """ flexible additional information for this build, used by content
    generators """

    id: int
    """ Same as build_id """

    name: str
    """ The name component of the NVR of this build. Should match the
    package_name field. """

    nvr: str
    """ The unique NVR of the build, comprised of the name, version, and
    release separated by hyphens """

    owner_id: int
    """ ID of the koji user that owns this build """

    owner_name: str
    """ name of the koji user that owns this build """

    package_id: int
    """ The corresponding package ID for this build. """

    package_name: str
    """ The corresponding package name for this build. Should match the
    name field. """

    release: str
    source: str
    start_time: str
    start_ts: float

    state: int
    """ state of the build, see `BuildState` """

    task_id: int

    version: str
    """ version portion of the NVR for the build """

    volume_id: int
    """ ID of the storage volume that the archives for this build will be
    stored on """

    volume_name: str
    """ name of the storage that the archives for this build will be
    stored on """

    maven_group_id: Optional[str]
    """ only present on Maven builds which have been loaded with type
    information """

    maven_artifact_id: Optional[str]
    """ only present on Maven builds which have been loaded with type
    information """

    maven_version: Optional[str]
    """ only present on Maven builds which have been loaded with type
    information """

    platform: Optional[str]
    """ only present on Windows builds which have been loaded with type
    information """

    tag_name: Optional[str]
    """ only present in listTagged output"""

    create_event: Optional[int]
    """ only present in listTagged output"""


_BuildInfos: TypeAlias = Collection[_BuildInfo]
"""
An Collection of _BuildInfo dicts
"""


class _BuildrootInfo(TypedDict):
    arch: str
    br_type: int

    cg_id: Optional[int]
    cg_name: Optional[str]
    cg_version: Optional[str]

    container_arch: str
    container_type: str

    create_event_id: int
    create_event_time: str
    create_ts: float

    extra: Optional[dict]

    host_arch: Optional[str]
    host_id: int
    host_name: str
    host_os: Optional[str]

    id: int

    repo_create_event_id: int
    repo_create_event_time: str

    repo_id: int
    repo_state: int

    retire_event_id: int
    retire_event_time: str
    retire_ts: float

    state: int

    tag_id: int
    tag_name: str

    task_id: int

    workdir: str


_BuildRootInfos: TypeAlias = Collection[_BuildrootInfo]


class _BuildTarget(TypedDict):
    id: int
    build_tag: int
    dest_tag: int
    name: str
    build_tag_name: str
    dest_tag_name: str


class _ChannelInfo(TypedDict):
    id: int
    """ internal channel ID """

    name: str
    """ channel name """


_ChannelInfos: TypeAlias = Collection[_ChannelInfo]


class _CGInfo(TypedDict):
    """
    Data representing a koji Content Generator. A dict of these are
    typically obtained via the ``listCGs`` XMLRPC call, mapping their
    friendly names to the _CGInfo structure
    """

    id: int
    """ internal identifier """

    users: List[str]
    """ list of account names with access to perform CGImports using
    this content generator """


class _Event(TypedDict):
    id: int
    ts: float


class _ExternalRepo(TypedDict):
    id: int
    name: str
    url: str


_ExternalRepos: TypeAlias = Collection[_ExternalRepo]


class _HostInfo(TypedDict):
    """
    Data representing a koji host. These are typically obtained via the
    ``getHost`` XMLRPC call
    """

    arches: str
    """ space-separated list of architectures this host can handle """

    capacity: float
    """ maximum capacity for tasks, using the sum of the task weight
    values """

    comment: str
    """ text describing the current status or usage """

    description: str
    """ text describing this host """

    enabled: bool
    """ whether this host is configured by the hub to take tasks """

    id: int
    """ internal identifier """

    name: str
    """ user name of this host's account, normally FQDN. """

    ready: bool
    """ whether this host is reporting itself as active and prepared to
    accept tasks """

    task_load: float
    """ the load of currently running tasks on the host. Compared with the
    capacity and a given task's weight, this can determine whether a
    task will 'fit' on the host """

    user_id: int
    """ the user ID of this host's account. Hosts have a user account of
    type HOST, which is how they authenticate with the hub """


class _ListTaskOpts(TypedDict, total=False):
    """Specific filter dictionary for listTasks API call"""
    arch: Iterable[str]
    not_arch: Iterable[str]
    state: Iterable[int]
    not_state: Iterable[int]
    owner: Union[int, Iterable[int]]
    not_owner: Union[int, Iterable[int]]
    host_id: Union[int, Iterable[int]]
    not_host_id: Union[int, Iterable[int]]
    channel_id: Union[int, Iterable[int]]
    not_channel_id: Union[int, Iterable[int]]
    parent: Union[int, Iterable[int]]
    not_parent: Union[int, Iterable[int]]
    decode: bool
    method: str
    createdBefore: Union[float, str]
    createdAfter: Union[float, str]
    startedBefore: Union[float, str]
    startedAfter: Union[float, str]
    completeBeforer: Union[float, str]
    completeAfter: Union[float, str]


class _PackageInfo(TypedDict):
    """
    ``getPackage`` XMLRPC call.
    """

    id: int
    """
    the internal ID for this package
    """

    name: str
    """
    the package name
    """


class _PermInfo(TypedDict):
    id: int
    name: str


class _QueryOptsType(TypedDict, total=False):
    """Various API calls use queryOpts dictionary for altering output format"""
    countOnly: bool
    order: str
    offset: int
    limit: int
    group: str
    asList: bool


class _RepoInfo(TypedDict):
    """
    Data representing a koji build tag's repository. These are
    typically obtained via the ``getRepo`` or ``repoInfo`` XMLRPC
    calls.
    """

    create_event: int
    """ koji event ID representing the point that the repo's tag
    configuration was snapshot from. Note that this doesn't always
    correlate to the creation time of the repo -- koji has the ability to
    generate a repository based on older events """

    create_ts: float
    """ UTC timestamp indicating when this repo was created """

    creation_time: str
    """ ISO-8601 formatted UTC datetime stamp indicating when this repo
    was created """

    dist: bool
    """ whether this is a dist-repo or not """

    id: int
    """ internal ID for this repository """

    state: int
    """ the current state of this repository """

    tag_id: int
    """ ID of the tag from which this repo was generated. This value is not
    present in the output of the ``getRepo`` XMLRPC call as it is presumed
    that the caller already knows the tag's identity """

    tag_name: str
    """ name of the tag from which this repo was generated.  This value is
    not present in the output of the ``getRepo`` XMLRPC call as it is
    presumed that the caller already knows the tag's identity """

    task_id: int
    """ ID of the task which generated this repository """


class _RPMInfo(TypedDict):
    """
    Data representing a koji RPM. These are typically obtained via the
    ``listRPMs`` XMLRPC call, or from the `kojismokydingo.as_rpminfo`
    function
    """

    arch: str
    """ The RPM's architecture, eg. 'src' or 'x86_64' """

    build_id: int
    """ The ID of the build owning this RPM """

    buildroot_id: int
    """ The buildroot used by the task which produced this RPM """

    buildtime: int
    """ UTC timestamp of the time that this RPM was produced """

    epoch: str
    """ The RPM's epoch field, or None if not defined """

    external_repo_id: int
    """ The external repo ID for this RPM record, or 0 if the RPM was
    built in this koji instance rather than being a reference to an
    external repository """

    external_repo_name: str
    """ name identifying the repo that this RPM came from, or 'INTERNAL'
    if built in this koji instance """

    extra: dict
    """ Optional extra data """

    id: int
    """ The internal ID for this RPM """

    metadata_only: bool

    name: str
    """ The RPM's name field """

    nvr: str
    """ The NVR (Name Version and Release) of the RPM """

    payloadhash: str
    """ The MD5 in hex of the RPM's payload (the content past the
    headers) """

    release: str
    """ The RPM's release field """

    size: int
    """ The file size of the unsigned copy of the RPM """

    version: str
    """ The RPM's version field """


_RPMInfos = Collection[_RPMInfo]


class _RPMSignature(TypedDict):
    """
    Data representing an RPM signature in koji. Obtained via the
    ``queryRPMSigs`` XMLRPC API.
    """

    rpm_id: int
    sigkey: str
    sighash: str


class _SearchResult(TypedDict):
    """ as returned by the ``search`` XMLRPC call """

    id: int
    """ result ID """

    name: str
    """ result name """

_SearchResults: TypeAlias = Collection[_SearchResult]


class _TagGroupPackage(TypedDict):
    basearchonly: str
    blocked: bool
    group_id: int
    package: str
    requires: str
    tag_id: int
    type: str


class _TagGroupReq(TypedDict):
    blocked: bool
    group_id: int
    is_metapkg: bool
    name: str
    req_id: int
    tag_id: int
    type: str


class _TagInfo(TypedDict):
    """
    Data representing a koji tag. Typically obtained via the
    ``getTag`` XMLRPC call, or the `kojismokydingo.as_taginfo` and
    `kojismokydingo.bulk_load_tags` functions.
    """

    arches: str
    """ space-separated list of architectures, or None """

    extra: Dict[str, str]
    """ inheritable additional configuration data """

    id: int
    """ internal ID of this tag """

    locked: bool
    """ when locked, a tag will protest against having addtional builds
    associated with it """

    maven_include_all: bool
    """ whether this tag should use the alternative maven-latest logic
    (including multiple builds of the same package name) when inherited
    by the build tag of a maven-enabled target """

    maven_support: bool
    """ whether this tag should generate a maven repository when it is
    the build tag for a target """

    name: str

    perm: str
    """ name of the required permission to associate builds with this tag,
    or None """

    perm_id: int
    """ ID of the required permission to associate builds with this tag,
    or None """


_TagInfos = Collection[_TagInfo]


class _TagInheritanceEntry(TypedDict):
    """
    Data representing a single inheritance element. A list of these
    represents the inheritance data for a tag. Typically obtained via
    the ``getFullInheritance`` XMLRPC call.
    """

    child_id: int
    """ the ID of the child tag in the inheritance link. The child tag
    inherits from the parent tag """

    currdepth: int
    """ only present from the ``getFullInheritance`` call. The inheritance
    depth this link occurs at. A depth of 1 indicates that the child
    tag would be the one originally queried for its inheritance tree
    """

    filter: list
    """ only present from the ``getFullInheritance`` call. """

    intransitive: bool
    """ if true then this inheritance link would not be inherited. ie.
    this link only appears at a depth of 1, and is otherwise omitted. """

    maxdepth: int
    """ additional parents in the inheritance tree from this link are only
    considered up to this depth, relative from the link's current
    depth.  A maxdepth of 1 indicates that only the immediate parents
    will be inherited. A maxdepth of 0 indicates that the tag and none
    of its parents will be inherited. A value of None indicates no
    restriction. """

    name: str
    """ the parent tag's name """

    nextdepth: int
    """ only present from the ``getFullInheritance`` call. """

    noconfig: bool
    """ if True then this inheritance link does not include tag
    configuration data, such as extras and groups """

    parent_id: int
    """ the parent tag's internal ID """

    pkg_filter: str
    """ a regex indicating which package entries may be inherited. If empty,
    all packages are inherited """

    priority: int
    """ the inheritance link priority, which provides an ordering for
    links at the same depth with the same child tag (ie. what order
    the parent links for a given tag are processed in). Lower
    priorities are processed first. """


_TagInheritance: TypeAlias = Collection[_TagInheritanceEntry]
"""
As returned by the ``getInheritanceData`` and
``getFullInheritance`` XMLRPC calls. A list of inheritance elements
for a tag.
"""


class _TagPackageInfo(TypedDict):
    """
    ``listPackages`` XMLRPC call.
    """

    blocked: bool
    """ if True this entry represents a block """

    extra_arches: str
    """ additional architectures, separated by spaces """

    owner_id: int
    """ ID of the user who is the owner of the package for this tag """

    owner_name: str
    """ name of the user who is the owner of the package for this tag """

    package_id: int
    """ ID of the package """

    package_name: str
    """ name of the package """

    tag_id: int
    """ ID of the package listing's tag """

    tag_name: str
    """ name of the package listing's tag """


class _TargetInfo(TypedDict):
    """
    Data representing a koji build target. Typically obtained via the
    ``getBuildTarget`` or ``getBuildTargets`` XMLRPC calls, or the
    `kojismokydingo.as_targetinfo` function.
    """

    build_tag: int
    """ internal ID of the target's build tag """

    build_tag_name: str
    """ name of the target's build tag """

    dest_tag: int
    """ internal ID of the target's destination tag """

    dest_tag_name: str
    """ name of the target's destination tag """

    id: int
    """ internal ID of this build target """

    name: str
    """ name of this build target """


_TargetInfos = Collection[_TargetInfo]


class _TaskInfo(TypedDict):
    """
    ``getTaskInfo`` XMLRPC call or `kojismokydingo.as_taskinfo` function
    """

    arch: str
    """ task architecture, or 'noarch' """

    awaited: Union[bool, None]
    """ True if this task is currently being waiting-for by its parent
    task.  False if this task is no longer being waited-for. None if
    the task was never waited-for. """

    channel_id: int
    """ internal ID of the channel from which a host will be selected to
    take this task """

    completion_time: str
    """ ISO-8601 formatted UTC datetime stamp indicating when this task
    was completed, or None if not completed """

    completion_ts: float
    """ UTC timestamp indicating when this task was completed, or None if
    not completed """

    create_time: str
    """ ISO-8601 formatted UTC datetime stamp indicating when this task
    was created """

    create_ts: float
    """ UTC timestamp indicating when this task was created """

    host_id: int
    """ host which has taken this task, or None """

    id: int
    """ internal task ID """

    label: str
    """ task label, or None """

    method: str
    """ task method, indicates the type of work to be done """

    owner: int
    """ ID of the user that initiated this task """

    parent: int
    """ ID of the parent task, or None """

    priority: int

    start_time: str
    """ ISO-8601 formatted UTC datetime stamp indicating when this task
    was started by a host, or None if not yet started """

    start_ts: float
    """ UTC timestamp indicating when this task was started by a host, or
    None if not yet started """

    state: int
    """ the current state of this task """

    waiting: Union[bool, None]
    """ True if this task is currently waiting for any of its subtasks to
    complete. False if this task is not waiting, or None if the task
    never needed to wait. """

    weight: float
    """ value which ascribes the general resources needed to perform this
    task. hosts have a limit to the number of resources which can be used
    to run tasks in parallel """

    request: List[Any]
    """ The task request info. Only present when the request parameter to
    the ``getTaskInfo`` call is `True`. Note that the `as_taskinfo`
    function does set that parameter to True. """


_TaskInfos: TypeAlias = Collection[_TaskInfo]


class _UserInfo(TypedDict):
    """
    Data representing a koji user account. These are typically
    obtained via the ``getUser`` or ``getLoggedInUser`` XMLRPC calls,
    or the ``kojismokydingo.as_userinfo`` function.
    """

    authtype: int
    """ Only present from the ``getLoggedInUser`` call """

    id: int
    """ internal identifer """

    krb_principal: str
    """ kerberos principal associated with the user. Only used in koji
    before 1.19 or when using the ``getLoggedInUser`` call. """

    krb_principals: List[str]
    """ list of kerberos principals associated with the user. Used in koji
    from 1.19 onwards. """

    name: str
    """ the username """

    status: int
    """ status of the account. not present for members from the
    ``getGroupMembers`` call. """

    usertype: int
    """ type of the account """


_UserInfos: TypeAlias = Collection[_UserInfo]


class _Volume(TypedDict):
    id: int
    name: str


_Volumes: TypeAlias = Collection[_Volume]


class _BuildReferences(TypedDict, total=False):
    tags: _TagInfos
    rpms: _RPMInfos
    component_of: List[int]
    archives: List[int]
    last_used: Optional[int]


class _TagGroup(TypedDict):
    """
    ``getTagGroups`` XMLRPC call
    """

    biarchonly: bool
    blocked: bool
    description: str
    display_name: str
    exported: bool
    group_id: int
    grouplist: List[_TagGroupReq]
    is_default: bool
    langonly: str
    name: str
    packagelist: List[_TagGroupPackage]
    tag_id: int
    uservisible: bool


_TagGroups: TypeAlias = Collection[_TagGroup]


# specs
_ArchiveSpec = Union[int, str, _ArchiveInfo]
_BuildSpec: TypeAlias = Union[int, str, _BuildInfo]
_CGSpec: TypeAlias = Union[int, str]
_ChannelSpec = Union[int, str, _ChannelInfo]
_GroupSpec = Union[int, str]
_HostSpec = Union[int, str, _HostInfo]
_PackageSpec = Union[int, str, _PackageInfo]
_RepoSpec = Union[int, _RepoInfo, str, _TagInfo]
_RPMSpec = Union[int, str, _RPMInfo]
_TagSpec = Union[int, str, _TagInfo]
_TargetSpec = Union[int, str, _TargetInfo]
_UserSpec = Union[int, str, _UserInfo]

# koji/__init__.py part

class Enum(dict):
    def get(
            self,
            key: Union[str, int],
            default: Optional[Union[str, int]] = None):
        ...

    def getnum(
            self,
            key: str,
            default: Optional[int] = None) -> int:
        ...


AUTHTYPE_NORMAL: int
AUTHTYPE_KERB: int
AUTHTYPE_SSL: int
AUTHTYPE_GSSAPI: int


REPO_INIT: int
REPO_READY: int
REPO_EXPIRED: int
REPO_DELETED: int
REPO_PROBLEM: int
REPO_MERGE_MODES: Set[str]

RPM_SIGTAG_GPG: int
RPM_SIGTAG_PGP: int
RPM_SIGTAG_RSA: int
RPM_SIGTAG_MD5: int

RPM_TAG_HEADERSIGNATURES: int
RPM_TAG_FILEDIGESTALGO: int

PRIO_DEFAULT: int

BASEDIR: str


AUTHTYPES: Enum
BR_STATES: Enum
BR_TYPES: Enum
BUILD_STATES: Enum
CHECKSUM_TYPES: Enum
REPO_STATES: Enum
TAG_UPDATE_TYPES: Enum
TASK_STATES: Enum
USERTYPES: Enum
USER_STATUS: Enum


class Fault:
    def __init__(
            self,
            faultCode: int,
            faultString: str,
            **extra: Any):
        ...


class FaultInfo(TypedDict):
    faultCode: int
    faultString: str


class GenericError(Exception):
    faultCode: int
    fromFault: bool


class ActionNotAllowed(GenericError):
    ...


class AuthError(GenericError):
    ...


class BuildError(GenericError):
    ...


class BuildrootError(BuildError):
    ...


class ParameterError(GenericError):
    ...


class ConfigurationError(GenericError):
    ...


class LockError(GenericError):
    ...


class AuthExpired(AuthError):
    ...


class AuthLockError(AuthError):
    ...


class RetryError(AuthError):
    ...


class TagError(GenericError):
    ...


class LiveMediaError(GenericError):
    ...


class ApplianceError(GenericError):
    ...


class LiveCDError(GenericError):
    ...


class PathInfo:
    topdir: str

    def __init__(
            self,
            topdir: Optional[str] = None):
        ...

    def build(
            self,
            build: _BuildInfo) -> str:
        ...

    def build_logs(
            self,
            build: _BuildInfo) -> str:
        ...

    def distrepo(
            self,
            repo_id: int,
            tag: _TagInfo,
            volume: Optional[str]) -> str:
        ...

    def imagebuild(
            self,
            build: _BuildInfo) -> str:
        ...

    def mavenbuild(
            self,
            build: _BuildInfo) -> str:
        ...

    def mavenfile(
            self,
            maveninfo: _ArchiveInfo) -> str:
        ...

    def mavenrepo(
            self,
            maveninfo: _ArchiveInfo) -> str:
        ...

    def repo(
            self,
            repo_id: int,
            tag_str: str) -> str:
        ...

    def repocache(
            self,
            tag_str: str) -> str:
        ...

    def rpm(
            self,
            rpminfo: _RPMInfo) -> str:
        ...

    def scratch(self) -> str:
        ...

    def sighdr(
            self,
            rinfo: _RPMInfo,
            sigkey: str) -> str:
        ...

    def signed(
            self,
            rpminfo: _RPMInfo,
            sigkey: str) -> str:
        ...

    def task(
            self,
            task_id: int,
            volume: Optional[str] = None) -> str:
        ...

    def taskrelpath(
            self,
            task_id: int) -> str:
        ...

    def tmpdir(
            self,
            volume: Optional[str] = None) -> str:
        ...

    def typedir(
            self,
            build: _BuildInfo,
            btype: str) -> str:
        ...

    def volumedir(
            self,
            volume: Optional[str] = None) -> str:
        ...

    def winbuild(
            self,
            build: _BuildInfo) -> str:
        ...

    def winfile(
            self,
            wininfo: _ArchiveInfo) -> str:
        ...

    def work(
            self,
            volume: Optional[str] = None) -> str:
        ...


pathinfo: PathInfo
PathSpec = Union[str, PathInfo]


class MultiCallSession:
    def __enter__(self) -> Self:
        ...

    def __exit__(self, _type, value, traceback) -> bool:
        ...

    def __getattr__(self, name) -> Any:
        ...

    ...


class ClientSession:

    baseurl: str
    opts: Dict[str, Any]
    krb_principal: str
    logged_in: bool

    def multicall(
            self,
            strict: bool = False,
            batch: Optional[int] = None) -> MultiCallSession:
        ...

    def __init__(
            self,
            baseurl: str,
            opts: _Options = None,
            sinfo: _Options = None,
            auth_method: _Options = None):
        ...

    def fastUpload(
            self,
            localfile: str,
            path: str,
            name: Optional[str] = None,
            callback: Optional[Callable] = None,
            blocksize: Optional[int] = None,
            overwrite: bool = False,
            volume: Optional[str] = None):
        ...

    def new_session(self):
        ...

    def setSession(self, sinfo: dict):
        ...

    def subsession(self) -> Self:
        ...

    def uploadWrapper(
            self,
            localfile: str,
            path: str,
            name: Optional[str] = None,
            callback: Optional[Callable] = None,
            blocksize: Optional[int] = None,
            overwrite: bool = True,
            volume: Optional[str] = None) -> None:
        ...

    # API
    def _listapi(self) -> List[_StrDict]:
        ...

    def CGImport(
            self,
            metadata: Union[str, _StrDict],
            directory: str,
            token: Optional[str] = None) -> _BuildInfo:
        ...

    def addChannel(
            self,
            channel_name: str,
            description: Optional[str] = None) -> int:
        ...

    def addExternalRepoToTag(
            self,
            tag_info: _TagSpec,
            repo_info: _RepoSpec,
            priority: int,
            merge_mode: str = 'koji',
            arches: Optional[str] = None) -> None:
        ...

    def addHost(
            self,
            hostname: str,
            arches: List[str],
            krb_principal: Optional[str] = None,
            force: bool = False) -> int:
        ...

    def addHostToChannel(
            self,
            hostname: _HostSpec,
            channel_name: str,
            create: bool = False,
            force: bool = False) -> None:
        ...

    def addRPMSig(
            self,
            an_rpm: _RPMSpec,
            data: str) -> None:
        ...

    def addVolume(
            self,
            name: str,
            strict: bool = True) -> _Volume:
        ...

    def applyVolumePolicy(
            self,
            build: _BuildInfo,
            strict: bool = False) -> None:
        ...

    def assignTask(
            self,
            task_id: int,
            hostname: str,
            force: bool = False) -> bool:
        ...

    def build(
            self,
            src: str,
            target: _TargetSpec,
            opts: Optional[Dict] = None,
            priority: Optional[int] = None,
            channel: Optional[str] = None) -> int:
        ...

    def buildImage(
            self,
            name: str,
            version: str,
            arch: _ArchesType,
            target: str,
            ksfile: str,
            img_type: str,
            opts: _StrDict,
            priority: Optional[int] = None) -> int:
        ...

    def buildImageOz(
            self,
            name: str,
            version: str,
            arches: _ArchesType,
            target: str,
            inst_tree: str,
            opts: _StrDict,
            priority: Optional[int] = None) -> int:
        ...

    def buildImageIndirection(
            self,
            opts: _Options = None,
            priority: Optional[int] = None) -> int:
        ...

    def buildReferences(
            self,
            build: _BuildInfo,
            limit: Optional[int] = None,
            lazy: bool = False) -> _BuildReferences:
        ...

    def chainBuild(
            self,
            srcs: List[List[str]],
            target: str,
            opts: _Options = None,
            priority: Optional[int] = None,
            channel: Optional[str] = None) -> int:
        ...

    def chainMaven(
            self,
            builds: _BuildInfos,
            target: str,
            opts: _Options = None,
            priority: Optional[int] = None,
            channel: str = 'maven') -> int:
        ...

    def changeBuildVolume(
            self,
            build: _BuildSpec,
            volume: str,
            strict: bool = True) -> None:
        ...

    def count(
            self,
            methodName: str,
            *args: Any,
            **kw: Any) -> int:
        ...

    def createBuildTarget(
            self,
            name: str,
            build_tag: _TagSpec,
            dest_tag: _TagSpec) -> None:
        ...

    def createEmptyBuild(
            self,
            name: str,
            version: str,
            release: str,
            epoch: str,
            owner: Optional[_UserSpec] = None) -> int:
        ...

    def createImageBuild(
            self,
            build_info: Union[str, _StrDict]) -> None:
        ...

    def createMavenBuild(
            self,
            build_info: Union[str, _StrDict],
            maven_info: _StrDict) -> None:
        ...

    def createNotification(
            self,
            user_id: int,
            package_id: Optional[int],
            tag_id: Optional[int],
            success_only: bool) -> None:
        ...

    def createNotificationBlock(
            self,
            user_id: int,
            package_id: Optional[int] = None,
            tag_id: Optional[int] = None) -> None:
        ...

    def createTag(
            self,
            name: str,
            parent: Optional[Union[int, str]] = None,
            arches: Optional[str] = None,
            perm: Optional[str] = None,
            locked: bool = False,
            maven_support: bool = False,
            maven_include_all: bool = False,
            extra: Optional[Dict[str, str]] = None) -> int:
        ...

    def createUser(
            self,
            username: str,
            status: Optional[int] = None,
            krb_principal: Optional[str] = None) -> int:
        ...

    def createWinBuild(
            self,
            build_info: Union[str, _StrDict],
            win_info: _StrDict) -> None:
        ...

    def deleteBuild(
            self,
            build: _BuildInfo,
            strict: bool = False,
            min_ref_age: int = 604800) -> bool:
        ...

    def deleteBuildTarget(
            self,
            buildTargetInfo: _TargetSpec) -> None:
        ...

    def deleteExternalRepo(
            self,
            info: _RepoSpec) -> None:
        ...

    def deleteNotification(
            self,
            id: int) -> None:
        ...

    def deleteNotificationBlock(self, id: int) -> None:
        ...

    def deleteRPMSig(
            self,
            rpminfo: _RPMSpec,
            sigkey: Optional[str] = None,
            all_sigs: bool = False) -> None:
        ...

    def deleteTag(self, tagInfo: _TagSpec) -> None:
        ...

    def disableHost(self, hostname: str) -> None:
        ...

    def disableUser(self, username: str) -> None:
        ...

    def distRepo(
            self,
            tag: _TagSpec,
            keys: List[str],
            **task_opts) -> int:
        ...

    def downloadTaskOutput(
            self,
            taskID: int,
            fileName: str,
            offset: int = 0,
            size: int = -1,
            volume: Optional[str] = None) -> bytes:
        ...

    def echo(self, *args) -> list:
        ...

    def editBuildTarget(
            self,
            buildTargetInfo: _TargetSpec,
            name: str,
            build_tag: _TagSpec,
            dest_tag: _TagSpec) -> None:
        ...

    def editChannel(
            self,
            channelInfo: _ChannelSpec,
            name: Optional[str] = None,
            description: Optional[str] = None,
            comment: Optional[str] = None) -> bool:
        ...

    def editExternalRepo(
            self,
            name: str,
            url: Optional[str] = None) -> None:
        ...

    def editHost(
            self,
            hostInfo: _HostSpec,
            arches: Optional[List[str]] = None,
            capacity: Optional[float] = None,
            description: Optional[str] = None,
            comment: Optional[str] = None) -> bool:
        ...

    def editPermission(
            self,
            perm: str,
            description: str) -> None:
        ...

    def editTag2(
            self,
            taginfo: Union[int, str],
            **kwargs) -> None:
        ...

    def editTagExternalRepo(
            self,
            tag_info: _TagSpec,
            repo_info: _RepoSpec,
            priority: Optional[int] = None,
            merge_mode: Optional[str] = None,
            arches: Optional[_ArchesType] = None) -> bool:
        ...

    def editUser(
            self,
            userInfo: _UserSpec,
            name: Optional[str] = None,
            krb_principal_mappings: Optional[List[_StrDict]] = None) -> None:
        ...

    def enableHost(self, hostname: str) -> None:
        ...

    def enableUser(self, username: str) -> None:
        ...

    def exclusiveSession(self, *args, **kwargs) -> None:
        ...

    def freeTask(self, task_id: int) -> None:
        ...

    def getAllPerms(self) -> List[_PermInfo]:
        ...

    def getArchive(
            self,
            archive_id: _ArchiveSpec,
            strict: bool = False) -> _ArchiveInfo:
        ...

    def getArchiveType(
            self,
            filename: Optional[str] = None,
            type_name: Optional[str] = None,
            type_id: Optional[int] = None,
            strict: bool = False) -> _ArchiveTypeInfo:
        ...

    def getArchiveTypes(self) -> List[_ArchiveTypeInfo]:
        ...

    def getBuild(
            self,
            buildInfo: Union[int, str],
            strict: bool = False) -> _BuildInfo:
        ...

    def getBuildConfig(
            self,
            tag: _TagSpec,
            event: Optional[int] = None) -> _TagInfo:
        ...

    def getBuildNotification(
            self,
            id: int,
            strict: bool = False) -> Collection[_StrDict]:
        ...

    def getBuildNotifications(
            self,
            userID: Optional[int] = None) -> Collection[_StrDict]:
        ...

    def getBuildNotificationBlocks(
            self,
            userID: Optional[int] = None) -> Collection[_StrDict]:
        ...

    def getBuildTarget(
            self,
            info: Union[int, str],
            event: Optional[int] = None,
            strict: bool = False) -> _TargetInfo:
        ...

    def getBuildTargets(
            self,
            info: Optional[Union[int, str]] = None,
            event: Optional[int] = None,
            buildTagID: Optional[int] = None,
            destTagID: Optional[int] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> _TargetInfos:
        ...

    def getBuildType(
            self,
            buildInfo: Union[int, str],
            strict: bool = False) -> Dict[str, dict]:
        ...

    def getBuildroot(
            self,
            buildrootID: int,
            strict: bool = False) -> _BuildrootInfo:
        ...

    def getChangelogEntries(
            self,
            buildID: Optional[int] = None,
            taskID: Optional[int] = None,
            filepath: Optional[str] = None,
            author: Optional[str] = None,
            before: Optional[_Timestamp] = None,
            after: Optional[_Timestamp] = None,
            queryOpts: Optional[_QueryOptsType] = None,
            strict: bool = False) -> Sequence[_StrDict]:
        ...

    def getChannel(
            self,
            channelInfo: Union[int, str],
            strict: bool = False) -> _ChannelInfo:
        ...

    def getFullInheritance(
            self,
            tag: Union[int, str],
            event: Optional[int] = None,
            reverse: bool = False) -> _TagInheritance:
        ...

    def getInheritanceData(
            self,
            tag: Union[int, str],
            event: Optional[int] = None) -> _TagInheritance:
        ...

    def getGroupMembers(
            self,
            group: Union[int, str]) -> _UserInfos:
        ...

    def getHost(
            self,
            hostInfo: Union[int, str],
            strict: bool = False,
            event: Optional[int] = None) -> _HostInfo:
        ...

    def getKojiVersion(self) -> str:
        ...

    def getLastHostUpdate(
            self,
            hostID: int,
            ts: bool = False) -> Union[str, float, None]:
        ...

    def getLastEvent(
            self,
            before: Optional[float] = None) -> _Event:
        ...

    def getLatestBuilds(
            self,
            tag: Union[int, str],
            event: Optional[int] = None,
            package: Optional[Union[int, str]] = None,
            type: Optional[str] = None) -> List[_BuildInfo]:
        ...

    def getLatestMavenArchives(
            self,
            tag: Union[int, str],
            event: Optional[int] = None,
            inherit: bool = True) -> List[_ArchiveInfo]:
        ...

    def getLatestRPMS(
            self,
            tag: Union[int, str],
            package: Optional[Union[int, str]] = None,
            arch: Optional[str] = None,
            event: Optional[int] = None,
            rpmsigs: bool = False,
            type: Optional[str] = None) -> Tuple[List[_RPMInfo],
                                                 List[_BuildInfo]]:
        ...

    def getLoggedInUser(self) -> _UserInfo:
        ...

    def getMavenBuild(
            self,
            buildInfo: _BuildSpec,
            strict: bool = False) -> _StrDict:
        ...

    def getPackage(
            self,
            info: Union[int, str],
            strict: bool = False,
            create: bool = False) -> _PackageInfo:
        ...

    def getPackageID(
            self,
            name: str,
            strict: bool = False) -> int:
        ...

    def getPerms(self) -> List[str]:
        ...

    def getRepo(
            self,
            tag: Union[int, str],
            state: Optional[_RepoState] = None,
            event: Optional[int] = None,
            dist: bool = False) -> _RepoInfo:
        ...

    def getRPM(
            self,
            rpminfo: _RPMSpec,
            strict: bool = False,
            multi: bool = False) -> Union[_RPMInfo, List[_RPMInfo]]:
        ...

    def getTag(
            self,
            taginfo: Union[int, str],
            strict: bool = False,
            event: Optional[int] = None,
            blocked: bool = False) -> _TagInfo:
        ...

    def getTagID(
            self,
            info: _TagSpec,
            strict: bool = False,
            create: bool = False) -> int:
        ...

    def getTagExternalRepos(
            self,
            tag_info: Optional[_TagSpec] = None,
            repo_info: Optional[_RepoSpec] = None,
            event: Optional[int] = None) -> _ExternalRepos:
        ...

    def getTagGroups(
            self,
            tag: Union[int, str],
            event: Optional[int] = None,
            inherit: bool = True,
            incl_pkgs: bool = True,
            incl_reqs: bool = True,
            incl_blocked: bool = False) -> _TagGroups:
        ...

    @overload
    def getTaskInfo(
            self,
            task_id: int,
            request: bool = False,
            strict: bool = False) -> _TaskInfo:
        ...

    @overload
    def getTaskInfo(
            self,
            task_id: List[int],
            request: bool = False,
            strict: bool = False) -> _TaskInfos:
        ...


    def getTaskChildren(
            self,
            task_id: int,
            request: bool = False,
            strict: bool = False) -> _TaskInfos:
        ...

    def getTaskResult(
            self,
            taskId: int,
            raise_fault: bool = True) -> dict:
        ...

    def getUser(
            self,
            userInfo: Optional[_UserSpec] = None,
            strict: bool = False,
            krb_princs: bool = True) -> _UserInfo:
        ...

    def getUserPerms(
            self,
            userID: Optional[_UserSpec] = None) -> List[str]:
        ...

    def getVolume(
            self,
            volume: str,
            strict: bool = False) -> _Volume:
        ...

    def getWinBuild(
            self,
            buildInfo: _BuildSpec,
            strict: bool = False) -> _StrDict:
        ...

    def grantCGAccess(
            self,
            user: _UserSpec,
            cg: _CGSpec,
            create: bool = False) -> None:
        ...

    def grantPermission(
            self,
            userinfo: _UserSpec,
            permission: str,
            create: bool = False,
            description: Optional[str] = None) -> None:
        ...

    def groupListAdd(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            block: bool = False,
            force: bool = False,
            **opts) -> None:
        ...

    def groupListBlock(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec) -> None:
        ...

    def groupListRemove(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec) -> None:
        ...

    def groupPackageListAdd(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            pkg_name: str,
            block: bool = False,
            force: bool = False,
            **opts) -> None:
        ...

    def groupPackageListBlock(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            pkg_name: str) -> None:
        ...

    def groupPackageListUnblock(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            pkg_name: str) -> None:
        ...

    def groupReqListAdd(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            reqinfo: _GroupSpec,
            force: bool = False,
            **opts) -> None:
        ...

    def groupReqListBlock(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            reqinfo: _GroupSpec) -> None:
        ...

    def groupReqListUnblock(
            self,
            taginfo: _TagSpec,
            grpinfo: _GroupSpec,
            reqinfo: _GroupSpec) -> None:
        ...

    def gssapi_login(
            self,
            principal: Optional[str] = None,
            keytab: Optional[str] = None,
            ccache: Optional[str] = None,
            proxyuser: Optional[str] = None) -> bool:
        ...

    def hasPerm(
            self,
            perm: str,
            strict: bool = False) -> bool:
        ...

    def importArchive(
            self,
            filepath: str,
            buildinfo: Union[_StrDict, _BuildInfo],
            type: str,
            typeInfo: _StrDict) -> None:
        ...

    def importRPM(
            self,
            path: str,
            basename: str) -> None:
        ...

    def listArchives(
            self,
            buildID: Optional[int] = None,
            buildrootID: Optional[int] = None,
            componentBuildrootID: Optional[int] = None,
            hostID: Optional[int] = None,
            type: Optional[str] = None,
            filename: Optional[str] = None,
            size: Optional[int] = None,
            checksum: Optional[str] = None,
            typeInfo: Optional[dict] = None,
            queryOpts: Optional[_QueryOptsType] = None,
            imageID: Optional[int] = None,
            archiveID: Optional[int] = None,
            strict: bool = False) -> List[_ArchiveInfo]:
        ...

    def listBTypes(
            self,
            query: Optional[Dict[str, str]] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> List[_BTypeInfo]:
        ...

    def listBuildroots(
            self,
            hostID: Optional[int] = None,
            tagID: Optional[int] = None,
            state: Optional[int] = None,
            rpmID: Optional[int] = None,
            archiveID: Optional[int] = None,
            taskID: Optional[int] = None,
            buildrootID: Optional[int] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> BuildRootInfos:
        ...

    def listBuilds(
            self,
            packageID: Optional[_PackageSpec] = None,
            userID: Optional[_UserSpec] = None,
            taskID: Optional[int] = None,
            prefix: Optional[str] = None,
            state: Optional[int] = None,
            volumeID: Optional[int] = None,
            source: Optional[str] = None,
            createdBefore: Optional[_Timestamp] = None,
            createdAfter: Optional[_Timestamp] = None,
            completeBefore: Optional[_Timestamp] = None,
            completeAfter: Optional[_Timestamp] = None,
            type: Optional[str] = None,
            typeInfo: Optional[dict[str, Any]] = None,
            queryOpts: Optional[_QueryOptsType] = None,
            pattern: Optional[str] = None,
            cgID: Optional[int] = None) -> _BuildInfos:
        ...

    def listCGs(self) -> Dict[str, _CGInfo]:
        ...

    def listChannels(
            self,
            hostID: Optional[_HostSpec] = None,
            event: Optional[int] = None,
            enabled: Optional[bool] = None) -> _ChannelInfos:
        ...

    def listExternalRepos(
            self,
            info: Optional[Union[str, int]] = None,
            url: Optional[str] = None,
            event: Optional[int] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> _ExternalRepos:
        ...

    def listHosts(
            self,
            arches: Optional[_ArchesType] = None,
            channelID: Optional[int] = None,
            ready: Optional[bool] = None,
            enabled: Optional[bool] = None,
            userID: Optional[int] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> List[_HostInfo]:
        ...

    def listPackages(
            self,
            tagID: Optional[int] = None,
            userID: Optional[int] = None,
            pkgID: Optional[int] = None,
            prefix: Optional[str] = None,
            inherited: bool = False,
            with_dups: bool = False,
            event: Optional[int] = None,
            queryOpts: Optional[_QueryOptsType] = None,
            with_owners: bool = True) -> List[_TagPackageInfo]:
        ...

    def listRPMs(
            self,
            buildID: Optional[int] = None,
            buildrootID: Optional[int] = None,
            imageID: Optional[int] = None,
            componentBuildrootID: Optional[int] = None,
            hostID: Optional[int] = None,
            arches: Optional[str] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> List[_RPMInfo]:
        ...

    def listTagged(
            self,
            tag: Union[int, str],
            event: Optional[int] = None,
            inherit: bool = False,
            prefix: Optional[str] = None,
            latest: bool = False,
            package: Optional[_PackageSpec] = None,
            owner: Optional[_UserSpec] = None,
            type: Optional[str] = None) -> List[_BuildInfo]:
        ...

    def listTaggedArchives(
            self,
            tag: Union[int, str],
            event: Optional[int] = None,
            inherit: bool = False,
            latest: bool = False,
            package: Optional[Union[int, str]] = None,
            type: Optional[str] = None) -> Tuple[List[_ArchiveInfo],
                                                 List[_BuildInfo]]:
        ...

    def listTaggedRPMS(
            self,
            tag: _TagSpec,
            event: Optional[int] = None,
            inherit: bool = False,
            latest: Union[bool, int] = False,
            package: Optional[_PackageSpec] = None,
            arch: Optional[str] = None,
            rpmsigs: bool = False,
            owner: Optional[str] = None,
            type: Optional[str] = None,
            strict: bool = True,
            extra: bool = True) -> _RPMInfos:
        ...

    def listTags(
            self,
            build: Optional[Union[int, str]] = None,
            package: Optional[Union[int, str]] = None,
            perms: bool = True,
            queryOpts: Optional[_QueryOptsType] = None,
            pattern: Optional[str] = None) -> List[_TagInfo]:
        ...

    def listTasks(
            self,
            opts: Optional[_ListTaskOpts] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> _TaskInfos:
        ...

    def listVolumes(self) -> _Volumes:
        ...

    def login(
            self,
            opts: Optional[Dict[str, Any]] = None) -> bool:
        ...

    def logout(self, session_id: Optional[int] = None) -> None:
        ...

    def makeTask(self, *args, **kwargs) -> int:
        ...

    def massTag(
            self,
            tag: Union[int, str],
            builds: List[str]) -> None:
        ...

    def mavenBuild(
            self,
            url: str,
            target: str,
            opts: _Options = None,
            priority: Optional[int] = None,
            channel: str = 'maven') -> int:
        ...

    def mavenEnabled(self) -> bool:
        ...

    def moveBuild(
            self,
            tag1: _TagSpec,
            tag2: _TagSpec,
            build: _BuildSpec,
            force: bool = False) -> None:
        ...

    def moveAllBuilds(
            self,
            tag1: _TagSpec,
            tag2: _TagSpec,
            package: _PackageSpec,
            force: bool = False) -> List[int]:
        ...

    def multiCall(
            self,
            strict: bool = False,
            batch: Optional[int] = None) -> List[Union[FaultInfo, List[Any]]]:
        ...

    def newRepo(
            self,
            tag: _TagSpec,
            event: Optional[_Event] = None,
            src: bool = False,
            debuginfo: bool = False,
            separate_src: bool = False) -> int:
        ...

    def packageListAdd(
            self,
            taginfo: Union[int, str],
            pkginfo: str,
            owner: Optional[Union[int, str]] = None,
            block: Optional[bool] = None,
            exta_arches: Optional[str] = None,
            force: bool = False,
            update: bool = False):
        ...

    def packageListBlock(
            self,
            taginfo: _TagSpec,
            pkginfo: _PackageSpec,
            force: bool = False) -> None:
        ...

    def packageListRemove(
            self,
            taginfo: _TagSpec,
            pkginfo: _PackageSpec,
            force: bool = False) -> None:
        ...

    def packageListSetOwner(
            self,
            taginfo: _TagSpec,
            pkginfo: _PackageSpec,
            owner: str,
            force: bool = False) -> None:
        ...

    def queryHistory(
            self,
            tables: Optional[List[str]] = None,
            **kwargs: Any) -> Dict[str, List[Dict[str, Any]]]:
        ...

    def queryRPMSigs(
            self,
            rpm_id: Optional[int] = None,
            sigkey: Optional[str] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> List[_RPMSignature]:
        ...

    def removeExternalRepoFromTag(
            self,
            tag_info: _TagSpec,
            repo_info: _RepoSpec) -> None:
        ...

    def removeHostFromChannel(
            self,
            hostname: _HostSpec,
            channel_name: str) -> None:
        ...

    def repoInfo(
            self,
            repo_id: int,
            strict: bool = False) -> _RepoInfo:
        ...

    def restartHosts(
            self,
            priority: int = 5,
            options: _Options = None) -> int:
        ...

    def resubmitTask(
            self,
            taskID: int) -> int:
        ...

    def revokeCGAccess(
            self,
            user: _UserSpec,
            cg: _CGSpec) -> None:
        ...

    def revokePermission(
            self,
            userinfo: _UserSpec,
            permission: str) -> None:
        ...

    def search(
            self,
            terms: str,
            type: str,
            matchType: str,
            queryOpts: Optional[_QueryOptsType] = None) -> _SearchResults:
        ...

    def setInheritanceData(
            self,
            tag: Union[int, str],
            data: _TagInheritance,
            clear: bool = False) -> None:
        ...

    def setTaskPriority(
            self,
            task_id: int,
            priority: int,
            recurse: bool = True) -> None:
        ...

    def snapshotTag(
            self,
            src: _TagSpec,
            dst: _TagSpec,
            config: bool = True,
            pkgs: bool = True,
            builds: bool = True,
            groups: bool = True,
            latest_only: bool = True,
            inherit_builds: bool = True,
            event: Optional[int] = None,
            force: bool = False) -> None:
        ...

    def snapshotTagModify(
            self,
            src: _TagSpec,
            dst: _TagSpec,
            config: bool = True,
            pkgs: bool = True,
            builds: bool = True,
            groups: bool = True,
            latest_only: bool = True,
            inherit_builds: bool = True,
            event: Optional[int] = None,
            force: bool = False,
            remove: bool = False) -> None:
        ...

    def ssl_login(
            self,
            cert: Optional[str] = None,
            ca: Optional[str] = None,
            serverca: Optional[str] = None,
            proxyuser: Optional[str] = None) -> bool:
        ...

    def tagBuild(
            self,
            tag: _TagSpec,
            build: _BuildSpec,
            force: bool = False,
            fromtag: Optional[_TagSpec] = None) -> int:
        ...

    def tagBuildBypass(
            self,
            tag: Union[int, str],
            build: Union[int, str],
            force: bool = False,
            notify: bool = False) -> None:
        ...

    def tagChangedSinceEvent(
            self,
            event: int,
            taglist: List[int]) -> bool:
        ...

    def untagBuildBypass(
            self,
            tag: Union[int, str],
            build: Union[int, str],
            strict: bool = True,
            force: bool = False,
            notify: bool = False) -> None:
        ...

    def untaggedBuilds(
            self,
            name: Optional[str] = None,
            queryOpts: Optional[_QueryOptsType] = None) -> _BuildInfos:
        ...

    def updateNotification(
            self,
            id: int,
            package_id: Optional[int],
            tag_id: Optional[int],
            success_only: Optional[bool]) -> None:
        ...

    def winBuild(
            self,
            vm: str,
            url: str,
            target: str,
            opts: _StrDict,
            priority: Optional[int] = None,
            channel: str = 'vm') -> int:
        ...

    def wrapperRPM(
            self,
            build: _BuildSpec,
            url: str,
            target: str,
            priority: Optional[int] = None,
            channel: str = 'maven',
            opts: Optional[Dict] = None) -> int:
        ...

    def writeSignedRPM(
            self,
            an_rpm: _RPMSpec,
            sigkey: str,
            force: bool = False) -> None:
        ...

    def getExternalRepo(
            self,
            info: Union[str, int],
            strict: bool = False,
            event: Optional[int] = None) -> Optional[_RepoInfo]:
        ...

    def getExternalRepoList(
            self,
            tag_info: _TagSpec,
            event: Optional[int] = None) -> Collection[Dict]:
        ...

    def createExternalRepo(
            self,
            name: str,
            url: str) -> _ExternalRepo:
        ...


def convertFault(fault: Fault) -> GenericError:
    ...


def read_config(
        profile_name: str,
        user_config: Optional[str] = None) -> _StrDict:
    ...


def read_config_files(
        config_files: List[Union[str, Tuple[str, bool]]],
        raw: bool = False) -> Union[ConfigParser, RawConfigParser]:
    ...


def hex_string(s: str) -> str:
    ...


def load_json(filepath: str):
    ...


def dump_json(
        filepath: str,
        data: Any,
        indent: int = 4,
        sort_keys: bool = False) -> None:
    ...


class RawHeader:
    def __init__(self, data: bytes):
        ...

    def get(self, key: int, default: Any = None):
        ...

    def version(self) -> int:
        ...

    def dump(self) -> None:
        ...


def check_NVR(
        nvr: Union[str, Dict[str, Union[str, int]]],
        strict: bool = False) -> bool:
    ...


def check_NVRA(
        nvra: Union[str, Dict[str, Union[str, int]]],
        strict: bool = False) -> bool:
    ...


def parse_NVR(nvr: str) -> Dict[str, Union[str, int]]:
    ...


def parse_NVRA(nvra: str) -> Dict[str, Union[str, int]]:
    ...


def grab_session_options(options) -> Dict[str, Any]:
    ...


def parse_arches(
        arches: str,
        to_list: bool = False,
        strict: bool = False,
        allow_none: bool = False) -> Union[List[str], str]:
    ...


def canonArch(arch: str) -> str:
    ...


def is_debuginfo(name: str) -> bool:
    ...


def _fix_print(value: Union[str, bytes]) -> str:
    ...


def _open_text_file(path: str, mode: str = 'rt'):
    ...


def formatTime(
        value: Union[int, float, datetime, DateTime]) -> str:
    ...


def formatTimeLong(value: Any) -> str:
    ...


def openRemoteFile(
        relpath: str,
        topurl: Optional[str],
        topdir: Optional[str],
        tempdir: Optional[str]):
    ...


def get_rpm_headers(
        f: Any,
        ts: Optional[int] = None) -> bytes:
    ...


def get_header_field(
        hdr: bytes,
        name: str,
        src_arch: bool = False) -> Union[str, List[str]]:
    ...


def get_header_fields(
        X: Union[bytes, str],
        fields: Optional[Sequence[str]],
        src_arch: bool = False) -> Dict[str, Union[str, List[str]]]:
    ...


def get_rpm_header(
        f: Union[bytes, str],
        ts: Optional[int] = None) -> bytes:
    ...


def maven_info_to_nvr(maveinfo: Dict[str, Any]) -> Dict[str, Any]:
    ...


def genMockConfig(
        name: str,
        arch: str,
        managed: bool = True,
        repoid: Optional[int] = None,
        tag_name: Optional[str] = None,
        **opts) -> str:
    ...


def buildLabel(
        buildInfo: _BuildInfo,
        showEpoch: bool = False) -> str:
    ...


def fixEncoding(
        value: Any,
        fallback: str = 'iso8859-15',
        remove_nonprintable: bool = False) -> str:
    ...


def fix_encoding(
        value: str,
        fallback: str = 'iso8859-15',
        remove_nonprintable: bool = False) -> str:
    ...


def add_file_logger(
        logger: Any,
        fn: str) -> None:
    ...


def add_mail_logger(
        logger: Any,
        addr: str) -> None:
    ...


def add_sys_logger(logger: Any):
    ...


def remove_log_handler(logger: str, handler: Any):
    ...


def add_stderr_logger(Any) -> None:
    ...


def daemonize() -> None:
    ...


def parse_pom(
        path: Optional[str] = None,
        contents: Optional[str] = None) -> dict:
    ...


def pom_to_maven_info(pominfo: _StrDict) -> _StrDict:
    ...


def taskLabel(taskInfo: _StrDict) -> str:
    ...


def encode_args(*args, **opts) -> list:
    ...


def decode_args(*args) -> Tuple[list, dict]:
    ...


def decode_args2(args, names, strict: bool = True) -> dict:
    ...


def decode_int(n: Any) -> int:
    ...


def safe_xmlrpc_loads(s: str) -> dict:
    ...


def ensuredir(directory: str) -> str:
    ...


def multibyte(data: bytes) -> int:
    ...


def find_rpm_sighdr(path: str) -> Tuple[int, int]:
    ...


def rpm_hdr_size(
        f: Union[str, IO],
        ofs: Optional[int] = None) -> int:
    ...


def rip_rpm_sighdr(src: str) -> bytes:
    ...


def rip_rpm_hdr(src: str) -> bytes:
    ...


def get_sigpacket_key_id(sigpacket: bytes) -> str:
    ...


def get_sighdr_key(sighdr: bytes) -> Union[str, None]:
    ...


class SplicedSigStreamReader(io.RawIOBase):
    def __init__(
            self,
            path: str,
            sighdr: bytes,
            bufsize: int) -> None:
        ...

    def generator(self) -> Generator[bytes, bytes, None]:
        ...

    def readable(self) -> bool:
        ...

    def readinto(
            self,
            b: bytes) -> int:
        ...


def spliced_sig_reader(
        path: str,
        sighdr: bytes,
        bufsize: Optional[int] = 8192) -> io.BufferedReader:
    ...


def splice_rpm_sighdr(
        sighdr: bytes,
        src: str,
        dst: Optional[str] = None,
        bufsize: Optional[int] = 8192,
        callback: Optional[Callable] = None) -> str:
    ...


class POMHandler(xml.sax.handler.ContentHandler):
    def __init__(self, values: list, fields: dict) -> None:
        ...

    def startElement(self, name: str, attrs: dict) -> None:
        ...

    def characters(self, content: str) -> None:
        ...

    def endElement(self, name: str) -> None:
        ...

    def reset(self) -> None:
        ...


def mavenLabel(dict) -> str:
    ...


def make_groups_spec(
        grplist: List[_StrDict],
        name: str = 'buildsys-build',
        buildgroup: Optional[str] = None) -> str:
    ...


def generate_comps(
        groups: List[_StrDict],
        expand_groups: bool = False) -> str:
    ...


def format_exc_plus() -> str:
    ...


def request_with_retry(
        retries: int = 3,
        backoff_factor: float = 0.3,
        status_forcelist: Sequence[int] = (500, 502, 504, 408, 429),
        session: Optional[requests.Session] = None) -> requests.Session:
    ...


def downloadFile(
        url: str,
        path: Optional[str] = None,
        fo: Optional[IO] = None):
    ...


def check_rpm_file(rpmfile: Union[IO, str]):
    ...


def config_directory_contents(
        dir_name: str,
        strict: bool = False) -> List[str]:
    ...


def get_profile_module(
        profile_name: str,
        config: Optional[Values] = None) -> ModuleType:
    ...


def is_requests_cert_error(e: Exception) -> bool:
    ...


def is_conn_error(e: Exception) -> bool:
    ...


def removeNonprintable(value: str) -> str:
    ...


def fixEncodingRecurse(
        value: str,
        fallback: Optional[str] = 'iso8859-15',
        remove_nonprintable: bool = False):
    ...
#
# The end.
