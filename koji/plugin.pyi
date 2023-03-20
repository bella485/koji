from _typeshed import Incomplete
from koji.util import encode_datetime_recurse as encode_datetime_recurse
from typing import Callable, Optional

callbacks: dict[str, list[Callable]]


class PluginTracker:
    searchpath: Optional[str]
    prefix: str
    plugins: dict

    def __init__(
            self,
            path: Optional[str] = ...,
            prefix: str = ...) -> None:
        ...

    def load(
            self,
            name: str,
            path: Optional[str] = ...,
            reload: bool = ...) -> Callable
        ...

    def get(
            self,
            name: str) -> Callable:
        ...

    def pathlist(self, path: str) -> str:
        ...

    def pathlist(self, path: list[str]) -> list[str]:
        ...


def export(f: Callable) -> Callable:
    ...


def export_cli(f: Callable) -> Callable:
    ...


def export_as(alias: str) -> Callable:
    ...


def export_in(
        module,
        alias: Optional[str] = ...) -> Callable:
    ...


def callback(*cbtypes) -> Callable:
    ...


def ignore_error(f: Callable) -> Callable:
    ...


def convert_datetime(f: Callable) -> Callable:
    ...


def register_callback(
        cbtype: str,
        func: Callable) -> None:
    ...


def run_callbacks(
        cbtype: str,
        *args,
        **kws) -> None:
    ...
