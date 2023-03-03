import base64
import koji

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Self,
    TypedDict,
    Tuple,
    Union,
)


class _Event(TypedDict):
    id: int
    ts: float


class _TagInfo(TypedDict):
    arches: str
    extra: Dict[str, str]
    id: int
    locked: bool
    maven_include_all: bool
    maven_support: bool
    name: str
    perm: str
    perm_id: int

# koji/util.py follows


DATE_RE: Any
TIME_RE: Any


def md5_constructor(*args: Any, **kwargs: Any):
    ...


def deprecated(message: str) -> None:
    ...


def formatChangelog(entries: List[str]) -> str:
    ...


def parseTime(val: str) -> Optional[int]:
    ...


def checkForBuilds(
        session: koji.ClientSession,
        tag,
        builds,
        event,
        latest: bool = False) -> bool:
    ...


def duration(start: float) -> str:
    ...


def printList(lst: List[str]) -> str:
    ...


def base64encode(
        s: Union[str, bytes],
        as_bytes: bool = False):
    ...


base64decode = base64.b64decode


def decode_bytes(
        data: bytes,
        fallback: str = 'iso8859-15') -> str:
    ...


def multi_fnmatch(
        s: str,
        patterns: Union[List[str], str]) -> bool:
    ...


def dslice(
        dict_: Dict[str, Any],
        keys: Iterable[str],
        strict: bool = True) -> Dict[str, Any]:
    ...


def dslice_ex(
        dict_: Dict[str, Any],
        keys: Iterable[str],
        strict: bool = True) -> Dict[str, Any]:
    ...


class DataWalker:
    def __init__(
            self,
            data: Any,
            callback: Callable,
            kwargs: Optional[dict] = None) -> None:
        ...

    def walk(self):
        ...


def encode_datetime(value: Any) -> str:
    ...


def encode_datetime_recurse(value: Any):
    ...


def call_with_argcheck(
        func: Callable,
        args: list,
        kwargs: Optional[dict] = None):
    ...


def apply_argspec(
        argspec: Iterable,
        args: Iterable,
        kwargs: Optional[dict] = None) -> dict:
    ...


class HiddenValue:
    def __init__(self, value: Any) -> None:
        ...


class LazyValue:
    def __init__(
            self,
            func: Callable,
            args: list,
            kwargs: Optional[dict] = None,
            cache: bool = False) -> None:
        ...

    def get(self):
        ...


class LazyString(LazyValue):
    ...


def lazy_eval(value: Any):
    ...


class LazyDict(dict):
    def lazyset(
            self,
            key: Any,
            func: Any,
            args: list,
            kwargs: Optional[dict] = None,
            cache: bool = False) -> None:
        ...

    def get(self, *args: Any, **kwargs: Any):
        ...

    def copy(self) -> Self:
        ...

    def values(self) -> Iterable:
        ...

    def items(self) -> Iterable:
        ...

    def itervalues(self) -> None:
        ...

    def iteritems(self) -> None:
        ...

    def pop(self, key: Any, *args: Any, **kwargs: Any):
        ...

    def popitem(self) -> Tuple:
        ...


class LazyRecord:
    def __init__(self, base: Optional[Any] = None) -> None:
        ...

    def __getattribute__(self, name: Any):
        ...


def lazysetattr(
        object: Any,
        name: str,
        func: Callable,
        args: list,
        kwargs: Optional[dict] = None,
        cache: bool = False) -> None:
    ...


class _RetryRmtree(Exception):
    ...


def rmtree(
        path: str,
        logger: Optional[Any] = None) -> None:
    ...


def safer_move(src: str, dst: str) -> None:
    ...


def move_and_symlink(
        src: str,
        dst: str,
        relative: bool = True,
        create_dir: bool = False) -> None:
    ...


def joinpath(
        path: str,
        *paths: List[str]) -> str:
    ...


def eventFromOpts(
        session: koji.ClientSession,
        opts: Any) -> Optional[_Event]:
    ...


def filedigestAlgo(hdr: bytes) -> str:
    ...


def check_sigmd5(filename: str) -> bool:
    ...


def parseStatus(rv: int, prefix: Union[str, Iterable[str]]) -> str:
    ...


def isSuccess(rv: int) -> bool:
    ...


def setup_rlimits(
        opts: Any,
        logger: Optional[Any] = None) -> None:
    ...


class adler32_constructor:
    def __init__(
            self,
            arg: str = '') -> None:
        ...

    def update(self, arg: Union[str, bytes]) -> None:
        ...

    def digest(self) -> bytes:
        ...

    def hexdigest(self) -> str:
        ...

    def copy(self) -> Self:
        ...

    digest_size: int = 4
    block_size: int = 1


def tsort(parts: Iterable) -> list:
    ...


class MavenConfigOptAdapter:
    def __init__(
            self,
            conf: Any,
            section: str) -> None:
        ...


def maven_opts(
        values: dict,
        chain: bool = False,
        scratch: bool = False) -> dict:
    ...


def maven_params(
        config: Any,
        package: str,
        chain: bool = False,
        scratch: bool = False) -> dict:
    ...


def wrapper_params(
        config: Any,
        package: str,
        chain: bool = False,
        scratch: bool = False) -> dict:
    ...


def parse_maven_params(
        confs: Any,
        chain: bool = False,
        scratch: bool = False) -> dict:
    ...


def parse_maven_param(
        confs: Any,
        chain: bool = False,
        scratch: bool = False,
        section: Optional[str] = None) -> dict:
    ...


def parse_maven_chain(
        confs: Any,
        scratch: bool = False) -> dict:
    ...


def to_list(lst: Iterable) -> list:
    ...


def format_shell_cmd(
        cmd: List[str],
        text_width: int = 80) -> str:
    ...
