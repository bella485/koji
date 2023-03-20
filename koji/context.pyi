from _typeshed import Incomplete


class _data:
    ...


class ThreadLocal:
    def __init__(self) -> None:
        ...

    def __getattr__(self, key: str):
        ...

    def __setattr__(self, key: str, value):
        ...

    def __delattr__(self, key: str):
        ...


context: ThreadLocal
