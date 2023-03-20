from _typeshed import Incomplete
import io
import logging
from build.lib.koji import ClientSession
import koji
from koji.tasks import safe_rmtree, BaseTaskHandler
from koji.util import (
    adler32_constructor,
    base64encode,
    dslice,
    joinpath,
    parseStatus,
    to_list,
)
from typing import Any, Optional, Collection, Callable


def incremental_upload(
        session: koji.ClientSession,
        fname: str,
        fd: io.IOBase,
        path: str,
        retries: int = ...,
        logger: Optional[logging.Logger] = ...) -> None:
    ...


def fast_incremental_upload(
        session: koji.ClientSession,
        fname: str,
        fd: io.IOBase,
        path: str,
        retries: int,
        logger: logging.Logger) -> None:
    ...


def log_output(
        session: koji.ClientSession,
        path: str,
        args: Collection[str],
        outfile: str,
        uploadpath: str,
        cwd: Optional[str] = None,
        logerror: bool | int = ...,
        append: bool | int = ...,
        chroot: Optional[str] = ...,
        env: Optional[dict[str, Any]] = ...):
    ...


class SCM:
    types: dict[str, tuple[str]]

    @classmethod
    def is_scm_url(
            cls,
            url: str,
            strict: bool = ...):
        ...

    logger: logging.Logger
    url: str
    scheme: str
    user: str
    host: str
    repository: str
    module: str
    revision: str
    use_common: bool
    source_cmd: list[str]
    scmtype: str

    def __init__(
            self,
            url: str,
            allow_password: bool = ...) -> None:
        ...

    def get_info(
            self,
            keys: list[str] = ...):
        ...

    def assert_allowed(
            self,
            allowed: str = ...,
            session: Optional[koji.ClientSession] = ...,
            by_config: bool = ...,
            by_policy: bool = ...,
            policy_data: dict[str, Any] = ...) -> None:
        ...

    def assert_allowed_by_config(
            self,
            allowed: str) -> None:
        ...

    def assert_allowed_by_policy(
            self,
            session: koji.ClientSession,
            **extra_data) -> None:
        ...

    sourcedir: str

    def checkout(
            self,
            scmdir: str,
            session: Optional[koji.ClientSession] = ...,
            uploadpath: Optional[str] = ...,
            logfile: Optional[str] = ...):
        ...

    def get_source(self):
        ...


class TaskManager:
    options: Incomplete
    session: koji.ClientSession
    tasks: dict[int, dict]
    skipped_tasks: dict[int, float]
    pids: dict[int, int]
    subsessions: dict[int, int]
    handlers: dict[str, BaseTaskHandler]
    status: str
    restart_pending: bool
    ready: bool
    hostdata: dict
    task_load: float
    host_id: int
    start_ts: float
    logger: logging.Logger

    def __init__(
            self,
            options,
            session: koji.ClientSession) -> None:
        ...

    def findHandlers(
            self,
            vars: dict) -> None:
        ...

    def registerHandler(
            self,
            entry: BaseTaskHandler) -> None:
        ...

    def registerCallback(
            self,
            entry: BaseTaskHandler) -> None:
        ...

    def registerEntries(
            self,
            vars: dict) -> None:
        ...

    def scanPlugin(
            self,
            plugin) -> None:
        ...

    def shutdown(self) -> None:
        ...

    def updateBuildroots(
            self,
            nolocal: bool = ...) -> None:
        ...

    def updateTasks(self) -> None:
        ...

    def getNextTask(self):
        ...

    def checkAvailDelay(
            self,
            task: dict[str, Any],
            bin_avail: list,
            our_avail: float):
        ...

    def cleanDelayTimes(self) -> None:
        ...

    def cleanupTask(
            self,
            task_id: int,
            wait: bool = ...):
        ...

    def checkSpace(self):
        ...

    def readyForTask(self):
        ...

    def takeTask(
            self,
            task: dict):
        ...

    def forkTask(
            self,
            handler: BaseTaskHandler):
        ...

    def runTask(
            self,
            handler: BaseTaskHandler) -> None:
        ...
