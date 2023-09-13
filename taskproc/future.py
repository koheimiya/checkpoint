from __future__ import annotations
from typing import Generic, Mapping, Protocol, Sequence, Any, runtime_checkable, TypeVar
from typing_extensions import overload
from dataclasses import dataclass
import json

from .types import JsonDict


K = TypeVar('K')
T = TypeVar('T')
R = TypeVar('R', covariant=True)



@runtime_checkable
class Future(Protocol[R]):
    # def run_task(self) -> Rj
    #     ...

    def get_task_result(self) -> R:
        ...

    def to_json(self) -> JsonDict:
        ...


class FutureMapperMixin:

    @overload
    def __getitem__(self: Future[Sequence[T]], key: int) -> MappedFuture[T]: ...
    @overload
    def __getitem__(self: Future[Mapping[K, T]], key: K) -> MappedFuture[T]: ...
    def __getitem__(self: Future[Mapping[K, T] | Sequence[T]], key: int | K) -> MappedFuture[T]:
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

    def get_task_result(self) -> R:
        out = self.get_origin().get_task_result()
        for k in self.get_args():
            out = out[k]
        return out

    def to_json(self) -> JsonDict:
        out = self.get_origin().to_json()
        out['__key__'] = self.get_args()
        return out


@dataclass(frozen=True)
class Const(FutureMapperMixin, Generic[R]):
    value: R

    def get_task_result(self) -> R:
        return self.value

    def to_json(self) -> JsonDict:
        raise TypeError('Const cannot be serialized to json: {self.value=}')


class FutureJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Future):
            return o.to_json()
        else:
            # Let the base class default method raise the TypeError
            return super().default(o)
