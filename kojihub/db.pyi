from _typeshed import Incomplete
from typing import Any, Iterable, Optional, Callable, Tuple
import logging

context: Incomplete
POSITIONAL_RE: Any
NAMED_RE: Any
logger: Incomplete

class DBWrapper:
    cnx: Incomplete

    def __init__(self, cnx: Any) -> None:
        ...

    def __getattr__(self, key: Any):
        ...

    def cursor(self, *args: Any, **kw: Any):
        ...

    def close(self) -> None:
        ...


class CursorWrapper:
    cursor: Incomplete
    logger: logging.Logger
    def __init__(self, cursor) -> None: ...
    def __getattr__(self, key: str) -> Any: ...
    def fetchone(self, *args, **kwargs): ...
    def fetchall(self, *args, **kwargs): ...
    def quote(self, operation: str, parameters: dict[str, Any]) -> str: ...
    def preformat(self, sql: str, params: dict[str, Any]): ...
    def execute(self, operation: str, parameters=list, log_errors: bool = ...): ...


def provideDBopts(**opts: Any) -> None: ...
def setDBopts(**opts: Any) -> None: ...
def getDBopts() -> dict[str, Any]: ...
def connect() -> DBWrapper: ...


class QueryProcessor:
    iterchunksize: int
    columns: Optional[list[str]]
    aliases: Optional[list[str]]
    colsByAlias: dict[str, str]
    tables: Optional[list[str]]
    joins: Optional[list[str]]
    clauses: Optional[list[str]]
    cursors: int
    values: dict[str, Any]
    transform: Callable
    opts: dict[str, Any]
    enable_group: bool
    logger: logging.Logger

    def __init__(self,
                 columns: Optional[Iterable[str]] = ...,
                 aliases: Optional[Iterable[str]] = ...,
                 tables: Optional[Iterable[str]] = ...,
                 joins: Optional[Iterable[str]] = ...,
                 clauses: Optional[Iterable[str]] = ...,
                 values: Optional[dict[str, Any]] = ...,
                 transform: Optional[Callable] = ...,
                 opts: Optional[dict[str, Any]] = ...,
                 enable_group: bool = ...) -> None:
        ...

    def countOnly(self, count: int) -> None:
        ...

    def singleValue(self, strict: bool = ...) -> Optional[dict[str, Any]]:
        ...

    def execute(self) -> list[dict[str, Any]]:
        ...

    def iterate(self) -> Iterable[dict[str, Any]]:
        ...

    def executeOne(self, strict: bool = ...) -> Optional[dict[str, Any]]:
        ...

def get_event() -> int: ...
def nextval(sequence: str) -> int: ...
def currval(sequence: str) -> int: ...
def db_lock(name: str, wait: bool = ...) -> bool: ...

class Savepoint:
    name: str
    def __init__(self, name: str) -> None: ...
    def rollback(self) -> None: ...

class InsertProcessor:
    table: str
    data: dict[str, Any]
    rawdata: dict[str, Any]

    def __init__(self,
                 table: str,
                 data: Optional[dict[str, Any]] = ...,
                 rawdata: Optional[dict[str, Any]] = ...) -> None:
        ...

    def set(self, **kwargs: Any) -> None: ...
    def rawset(self, **kwargs: Any) -> None: ...
    def make_create(self,
                    event_id: Optional[int] = ...,
                    user_id: Optional[int] = ...) -> None:
        ...

    def dup_check(self) -> bool: ...
    def execute(self) -> int: ...


class UpdateProcessor:
    table: str
    data: dict[str, Any]
    rawdata: dict[str, Any]
    clauses: list[str]
    values: dict[str, Any]
    def __init__(self,
                 table: str,
                 data: Optional[dict[str, Any]] = ...,
                 rawdata: Optional[dict[str, Any]] = ...,
                 clauses: Optional[Iterable[str]] = ...,
                 values: Optional[dict[str, Any]] = ...) -> None:
        ...

    def get_values(self) -> dict[str, Any]: ...
    def set(self, **kwargs: Any) -> None: ...
    def rawset(self, **kwargs: Any) -> None: ...
    def make_revoke(self,
                    event_id: Optional[int] = ...,
                    user_id: Optional[int] = ...) -> None:
        ...

    def execute(self) -> int: ...


class DeleteProcessor:
    table: str
    clauses: list[str]
    values: dict[str, Any]
    def __init__(self,
                 table: str,
                 clauses: Optional[Iterable[str]] = ...,
                 values: Optional[dict[str, Any]] = ...) -> None:
        ...

    def get_values(self) -> dict[str, Any]: ...
    def execute(self) -> int: ...


class BulkInsertProcessor:
    table: str
    data: dict[str, Any]
    columns: list[str]
    strict: bool
    batch: int
    def __init__(self,
                 table: str,
                 data=Optional[list[dict[str, Any]]] = ...,
                 columns=Optional[list[str]] = ...,
                 strict=bool = ...,
                 batch=int = ...) -> None:
        ...

    def __str__(self) -> str: ...
    def _get_insert(self, data: list[dict[str, Any]]) -> Tuple[str, dict[str, Any]]: ...
    def __repr__(self) -> str: ...
    def add_record(self, **kwargs: Any) -> None: ...
    def execute(self) -> None: ...
    def _one_insert(self, data: dict[str, Any]) -> None: ...
