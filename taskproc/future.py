from __future__ import annotations
from collections import UserDict, UserList
from typing import Generic, Mapping, Protocol, Sequence, Any, runtime_checkable, TypeVar, Self
from typing_extensions import overload
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
    def run_task(self) -> R:
        ...

    def get_result(self) -> R:
        ...

    def to_json(self) -> JsonDict:
        ...

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        ...




class FutureMapperMixin:

    @overload
    def __getitem__(self: Future[Sequence[T]], key: int) -> MappedFuture[T]: ...
    @overload
    def __getitem__(self: Future[Mapping[K, T]], key: K) -> MappedFuture[T]: ...
    def __getitem__(self: Future[Mapping[K, T] | Sequence[T]], key: int | K) -> MappedFuture[T]:
        assert isinstance(key, (int, float, str, bool, type(None))), f"Non-JSON-able key for Future: {key=}"
        return MappedFuture(self, key)


@dataclass
class MappedFuture(FutureMapperMixin, Generic[R]):
    task: Future[Mapping[Any, R] | Sequence[R]]
    key: Any

    def run_task(self) -> R:
        raise TypeError('Should not be called.')

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
        out = self.get_origin().to_json()
        out['__future__'] = 'MappedFuture'
        out['__key__'] = self.get_args()
        return out

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return self.get_origin().get_workers()


@dataclass(frozen=True)
class Const(FutureMapperMixin, Generic[R]):
    value: R

    def __post_init__(self):
        assert _check_if_literal(self.value), f"Non-literal const value: {self.value=}"

    def run_task(self) -> R:
        return self.value

    def get_result(self) -> R:
        return self.value

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'Const', '__repr__': repr(self.value)})

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return {}


class FutureJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Future):
            return o.to_json()
        else:
            # Let the base class default method raise the TypeError
            return super().default(o)


def _check_if_literal(x):
    try:
        xx = eval(repr(x), {}, {})
    except:
        return False
    return x == xx


class FutureDict(UserDict[K, Future[R]]):

    def run_task(self) -> dict[K, R]:
        raise TypeError('Should not be called.')

    def get_result(self) -> dict[K, R]:
        return {k: v.get_result() for k, v in self.items()}

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'FutureDict', '__value__': {k: v.to_json() for k, v in self.items()}})

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return {f'{k}.{kk}': vv for k, v in self.items() for kk, vv in v.get_workers().items()}


class FutureList(UserList[Future[R]]):

    def run_task(self) -> list[R]:
        raise TypeError('Should not be called.')

    def get_result(self) -> list[R]:
        return [v.get_result() for v in self]

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'FutureList', '__value__': [v.to_json() for v in self]})

    def get_workers(self) -> dict[str, TaskHandlerProtocol]:
        return {f'{i}.{kk}': vv for i, v in enumerate(self) for kk, vv in v.get_workers().items()}


