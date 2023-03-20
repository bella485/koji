import types
import six.moves.xmlrpc_client as xmlrpc_client
from _typeshed import Incomplete
from typing import Callable, Optional, Union

getparser: Incomplete
loads: Incomplete
Fault: Incomplete
DateTime: Incomplete


class ExtendedMarshaller(xmlrpc_client.Marshaller):
    dispatch: Incomplete

    def dump_generator(
            self,
            value: types.GeneratorType,
            write: Callable) -> None:
        ...

    MAXI8: int
    MINI8: int

    def dump_int(
            self,
            value: int,
            write: Callable):
        ...

    def dump_re(
            self,
            value,
            write: Callable):
        ...


def dumps(
        params,
        methodname: Optional[str] = ...,
        methodresponse: Optional[Union[bool, int]] = ...,
        encoding: Optional[str] = ...,
        allow_none: Union[bool, int] = ...,
        marshaller: Optional[xmlrpc_client.Marshaller] = ...) -> str:
    ...
