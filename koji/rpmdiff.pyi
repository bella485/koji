import json
from typing import Any, Optional


class BytesJSONEncoder(json.JSONEncoder):

    def default(self, o):
        ...


class Rpmdiff:
    TAGS: tuple[int]
    PRCO: tuple[str]
    PREREQ_FLAG: int
    DEPFORMAT: str
    FORMAT: str
    ADDED: str
    REMOVED: str
    result: list[tuple]
    old_data: dict[str, Any]
    new_data: dict[str, Any]

    def __init__(
            self,
            old: str,
            new: str,
            ignore: Optional[str] = ...) -> None:
        ...

    def textdiff(self) -> str:
        ...

    def differs(self) -> bool:
        ...

    def sense2str(
            self,
            sense: int) -> str:
        ...

    def kojihash(
            self,
            new: bool = ...) -> str:
        ...
