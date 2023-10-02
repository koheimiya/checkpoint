from __future__ import annotations
from abc import ABC, abstractmethod
from collections import UserDict, UserList
from typing import Generic, Hashable, Mapping, Sequence, Any, TypeVar
from typing_extensions import Protocol, overload
from dataclasses import dataclass
import json

from .types import JsonDict
from .graph import TaskWorkerProtocol


K = TypeVar('K', bound=Hashable)
T = TypeVar('T')
R = TypeVar('R', covariant=True)
P = TypeVar('P', contravariant=True)


class FutureProtocol(Protocol[R]):
    def get_result(self) -> R:
        ...

    def to_json(self) -> JsonDict:
        ...

    def get_workers(self, prefix: str) -> dict[str, TaskWorkerProtocol]:
        ...


class Future(Generic[R], ABC):
    @abstractmethod
    def get_result(self) -> R:
        ...

    @abstractmethod
    def to_json(self) -> JsonDict:
        ...

    @abstractmethod
    def get_workers(self, prefix: str) -> dict[str, TaskWorkerProtocol]:
        ...

    @overload
    def __getitem__(self: Future[Sequence[T]], key: int) -> MappedFuture[T]: ...
    @overload
    def __getitem__(self: Future[Mapping[K, T]], key: K) -> MappedFuture[T]: ...
    def __getitem__(self: Future[Mapping[K, T] | Sequence[T]], key: int | K) -> MappedFuture[T]:
        assert _check_if_literal(key), f"Non-literal key for Future: {key=}"
        return MappedFuture(self, key)


class FutureJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Future):
            return o.to_json()
        else:
            return super().default(o)


# class FutureMapperMixin:
#     @overload
#     def __getitem__(self: Future[Sequence[T]], key: int) -> MappedFuture[T]: ...
#     @overload
#     def __getitem__(self: Future[Mapping[K, T]], key: K) -> MappedFuture[T]: ...
#     def __getitem__(self: Future[Mapping[K, T] | Sequence[T]], key: int | K) -> MappedFuture[T]:
#         assert _check_if_literal(key), f"Non-literal key for Future: {key=}"
#         return MappedFuture(self, key)


@dataclass
class MappedFuture(Future[R]):
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

    def get_workers(self, prefix: str) -> dict[str, TaskWorkerProtocol]:
        return self.get_origin().get_workers(prefix)


@dataclass(frozen=True)
class Const(Future[R]):
    value: R

    def __post_init__(self):
        assert _check_if_literal(self.value), f"Non-literal const value: {self.value=}"

    def get_result(self) -> R:
        return self.value

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'Const', '__value__': repr(self.value)})

    def get_workers(self, prefix: str) -> dict[str, TaskWorkerProtocol]:
        return {}


def _check_if_literal(x: Any) -> bool:
    try:
        xx = eval(repr(x), {}, {})
    except:
        return False
    return x == xx


class FutureDict(UserDict[K, Future[T]], Future[Mapping[K, T]]):
    def __init__(self, __dict: Mapping[K, FutureProtocol[T]]):
        super().__init__(__dict)  # type: ignore

    def get_result(self) -> dict[K, T]:
        return {k: v.get_result() for k, v in self.items()}

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'FutureDict', '__value__': {k: v.to_json() for k, v in self.items()}})

    def get_workers(self, prefix: str) -> dict[str, TaskWorkerProtocol]:
        assert all('/' not in f'{k}' for k in self), 'Future key must not contain "/".'
        return {kk: vv for k, v in self.items() for kk, vv in v.get_workers(prefix=f'{prefix}.{k}').items()}


class FutureList(UserList[Future[T]], Future[Sequence[T]]):
    def __init__(self, __list: Sequence[FutureProtocol[T]]):
        super().__init__(__list)  # type: ignore

    def get_result(self) -> list[T]:
        return [v.get_result() for v in self]

    def to_json(self) -> JsonDict:
        return JsonDict({'__future__': 'FutureList', '__value__': [v.to_json() for v in self]})

    def get_workers(self, prefix: str) -> dict[str, TaskWorkerProtocol]:
        return {kk: vv for i, v in enumerate(self) for kk, vv in v.get_workers(f'{prefix}.{i}').items()}


