from __future__ import annotations
from collections import UserDict, UserList
from typing import Generic, Mapping, Protocol, Sequence, Any, runtime_checkable, TypeVar
from typing_extensions import overload, Self
from dataclasses import dataclass
import json

from .types import JsonDict
from .graph import TaskHandlerProtocol


K = TypeVar('K', int, float, str, bool, None)
T = TypeVar('T')
R = TypeVar('R', covariant=True)
P = TypeVar('P', contravariant=True)


@runtime_checkable
class Future(Protocol[R]):
    def get_result(self) -> R:
        ...

    def to_json(self) -> JsonDict:
        ...

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        ...


class FutureJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Future):
            return o.to_json()
        else:
            return super().default(o)


class FutureMapperMixin:
    @overload
    def __getitem__(self: Future[Sequence[T]], key: int) -> MappedFuture[T]: ...
    @overload
    def __getitem__(self: Future[Mapping[K, T]], key: K) -> MappedFuture[T]: ...
    def __getitem__(self: Future[Mapping[K, T] | Sequence[T]], key: int | K) -> MappedFuture[T]:
        assert _check_if_literal(key), f"Non-literal key for Future: {key=}"
        return MappedFuture(self, key)


@dataclass
class MappedFuture(FutureMapperMixin, Generic[R]):
    task: Future[Mapping[Any, R] | Sequence[R]]
    key: Any

    def get_origin(self) -> Future[Any]:
        x = self.task
        if isinstance(x, MappedFuture):
            return x.get_origin()
        else:
            return x

    def get_args(self) -> list[Any]:
        out = []
        x = self
        while isinstance(x, MappedFuture):
            out.append(x.key)
            x = x.task
        return out[::-1]

    def get_result(self) -> R:
        out = self.get_origin().get_result()
        for k in self.get_args():
            out = out[k]
        return out

    def to_json(self) -> JsonDict:
        out = JsonDict({
            '__future__': 'MappedFuture',
            '__orig__': self.get_origin().to_json(),
            '__args__': repr(self.get_args()),
            })
        return out

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return self.get_origin().get_workers()


@dataclass(frozen=True)
class Const(FutureMapperMixin, Generic[R]):
    value: R

    def __post_init__(self):
        assert _check_if_literal(self.value), f"Non-literal const value: {self.value=}"

    def get_result(self) -> R:
        return self.value

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'Const', '__value__': repr(self.value)})

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return {}


def _check_if_literal(x: Any) -> bool:
    try:
        xx = eval(repr(x), {}, {})
    except:
        return False
    return x == xx


class FutureDict(UserDict[K, Future[T]]):
    def get_result(self) -> dict[K, T]:
        return {k: v.get_result() for k, v in self.items()}

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'FutureDict', '__value__': {k: v.to_json() for k, v in self.items()}})

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return {f'{k}.{kk}': vv for k, v in self.items() for kk, vv in v.get_workers().items()}


class FutureList(UserList[Future[T]]):
    def get_result(self) -> list[T]:
        return [v.get_result() for v in self]

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'FutureList', '__value__': [v.to_json() for v in self]})

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return {f'{i}.{kk}': vv for i, v in enumerate(self) for kk, vv in v.get_workers().items()}


