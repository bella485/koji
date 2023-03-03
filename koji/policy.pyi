from typing import (
    Optional, Dict, Callable, List, Iterable, Tuple
)


class BaseSimpleTest:
    name: Optional[str] = None

    def __init__(self, str: str) -> None:
        ...

    def run(self, data: dict) -> None:
        ...


class TrueTest(BaseSimpleTest):
    name: str

    def run(self, data: dict) -> bool:
        ...


class FalseTest(BaseSimpleTest):
    ...


class AllTest(TrueTest):
    ...


class NoneTest(FalseTest):
    ...


class HasTest(BaseSimpleTest):
    field: Optional[str]
    ...


class BoolTest(BaseSimpleTest):
    field: Optional[str]
    ...


class MatchTest(BaseSimpleTest):
    field: Optional[str]
    ...


class TargetTest(MatchTest):
    ...


class CompareTest(BaseSimpleTest):
    allow_float: bool = True
    operators: Dict[str, Callable]


class SimpleRuleSet:

    def __init__(
            self,
            rules: Iterable[str],
            tests: Dict[str, Callable]) -> None:
        ...

    def parse_rules(
            self,
            lines: Iterable[str]) -> None:
        ...

    def parse_line(
            self,
            line: str) -> Tuple[List[Callable], bool, str]:
        ...

    def get_test_handler(self, str: str):
        ...

    def all_actions(self) -> List[str]:
        ...

    def apply(self, data: dict) -> str:
        ...

    def last_rule(self) -> str:
        ...


def findSimpleTests(namespace: str) -> Dict[str, BaseSimpleTest]:
    ...
