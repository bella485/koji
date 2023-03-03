from kojismokydingo.types import TaskInfo
from typing import Any, Optional, List, Union, Dict, TypedDict


class _HostInfo(TypedDict):
    arches: str
    capacity: float
    comment: str
    description: str
    enabled: bool
    id: int
    name: str
    ready: bool
    task_load: float
    user_id: int


class _RepoInfo(TypedDict):
    create_event: int
    create_ts: float
    creation_time: str
    dist: bool
    id: int
    state: int
    tag_id: int
    tag_name: str
    task_id: int


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


def scan_mounts(topdir: str) -> List[str]:
    ...


def umount_all(topdir: str) -> None:
    ...


def safe_rmtree(
        path: str,
        unmount: bool = False,
        strict: bool = True) -> int:
    ...


class ServerExit(Exception):
    ...


class ServerRestart(Exception):
    ...


def parse_task_params(
        method: str,
        params: Union[dict, str]) -> dict[str, Any]:
    ...


LEGACY_SIGNATURES: Dict[str, list]


class BaseTaskHandler:
    Methods: List[str]
    Foreground: bool = ...
    id: Any = ...
    method: Any = ...
    session: Any = ...
    options: Any = ...
    workdir: Any = ...
    logger: Any = ...
    manager: Any = ...

    def __init__(
            self,
            id: int,
            method: str,
            params: list,
            session: Any,
            options: dict,
            workdir: Optional[str] = ...) -> None:
        ...

    def setManager(self, manager: Any) -> None:
        ...

    def handler(self) -> None:
        ...

    def run(self) -> None:
        ...

    def weight(self) -> float:
        ...

    def createWorkdir(self) -> None:
        ...

    def removeWorkdir(self) -> None:
        ...

    def wait(
            self,
            subtasks: Optional[List[int]] = None,
            all: bool = False,
            failany: bool = False,
            canfail: Optional[bool] = None,
            timeout: Optional[int] = None) -> dict:
        ...

    def getUploadDir(self) -> str:
        ...

    def uploadFile(
            self,
            filename: str,
            relPath: Optional[str] = None,
            remoteName: Optional[str] = None,
            volume: Optional[str] = None) -> None:
        ...

    def uploadTree(
            self,
            dirpath: str,
            flatten: bool = False,
            volume: Optional[str] = None) -> None:
        ...

    def chownTree(
            self,
            dirpath: str,
            uid: int,
            gid: int) -> None:
        ...

    def localPath(
            self,
            relpath: str) -> str:
        ...

    def subtask(
            self,
            method: str,
            arglist: list,
            **opts: Any) -> int:
        ...

    def subtask2(
            self,
            __taskopts: list,
            __method: str,
            *args: list,
            **kwargs: dict) -> int:
        ...

    def find_arch(
            self,
            arch: str,
            host: _HostInfo,
            tag: _TagInfo,
            preferred_arch: Optional[str] = None) -> str:
        ...

    def getRepo(
            self,
            tag: Any,
            builds: Optional[Any] = None,
            wait: bool = False) -> _RepoInfo:
        ...

    def run_callbacks(
            self,
            plugin: str,
            *args: list,
            **kwargs: dict) -> None:
        ...

    @property
    def taskinfo(self) -> TaskInfo:
        ...

    @taskinfo.setter
    def taskinfo(self, taskinfo: TaskInfo) -> None:
        ...


class FakeTask(BaseTaskHandler):
    Methods: List[str]
    Foreground: bool = ...

    def handler(self, *args: list):
        ...


class SleepTask(BaseTaskHandler):
    def handler(self, n: int) -> None:
        ...


class ForkTask(BaseTaskHandler):
    def handler(self, n: int = 5, m: int = 37) -> None:
        ...


class WaitTestTask(BaseTaskHandler):
    def handler(self, count: int, seconds: int = ...) -> None:
        ...


class SubtaskTask(BaseTaskHandler):
    def handler(self, n: int = ...) -> None:
        ...


class DefaultTask(BaseTaskHandler):
    def handler(self, *args: list, **opts: dict) -> None:
        ...


class ShutdownTask(BaseTaskHandler):
    def handler(self) -> None:
        ...


class RestartTask(BaseTaskHandler):
    def handler(self, host: _HostInfo) -> str:
        ...


class RestartVerifyTask(BaseTaskHandler):
    def handler(self, task_id: int, host: _HostInfo) -> None:
        ...


class RestartHostsTask(BaseTaskHandler):
    def handler(self, options: Optional[Dict[str, Any]] = ...) -> None:
        ...


class DependantTask(BaseTaskHandler):
    def handler(
            self,
            wait_list: List[int],
            task_list: List[int]) -> None:
        ...


class MultiPlatformTask(BaseTaskHandler):
    def buildWrapperRPM(
            self,
            spec_url: str,
            build_task_id: int,
            build_target: int,
            build: int,
            repo_id: int,
            **opts: Dict[str, Any]):
        ...
