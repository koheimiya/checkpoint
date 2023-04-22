from __future__ import annotations
import inspect
from dataclasses import dataclass, field
from typing import Callable, ClassVar, Generic, TypeVar, ParamSpec, Any
import logging
import time
from datetime import datetime
import json
from typing_extensions import Self

from .env import DISABLE_CHECKPOINT_REUSE
from .base_db import Json, DBProtocol


LOGGER = logging.getLogger(__name__)


T = TypeVar('T')
P = ParamSpec('P')


@dataclass
class _Result(Generic[T]):
    value: T
    duration: float


@dataclass
class FunctionWithDB(Generic[P, T]):
    name: str
    func: Callable[P, T]
    db: DBProtocol[_Result[T], tuple[Self, Json]]
    cache_stats: dict[Json, tuple[int, int]] = field(default_factory=dict)  # hit and miss

    name_register: ClassVar[set[str]] = set()
    active_tasks: ClassVar[list[tuple[Self, Json]]] = []

    def __post_init__(self) -> None:
        self.name_register.add(self.name)

    @classmethod
    def make(cls, name: str | None, func: Callable[P, T], db_factory: Callable[[str], DBProtocol]) -> Self:
        if name is None:
            name = f'{func.__module__}.{func.__name__}'
        if name in cls.name_register:
            raise ValueError(f'{name} already exists')

        db = db_factory(name)
        return FunctionWithDB(name=name, func=func, db=db)

    def _serialize_arguments(self, *args: P.args, **kwargs: P.kwargs) -> Json:
        params = inspect.signature(self.func).bind(*args, **kwargs)
        params.apply_defaults()
        arguments = params.arguments
        return _serialize(arguments)

    def _run_and_save(self, arg_key: Json) -> datetime:
        """ Actually call the function with serialized arguments """
        # Push current task to stack and reset dependencies
        self.active_tasks.append((self, arg_key))
        self.db.reset_deps(arg_key)

        # Run func
        start_time = time.time()
        value = self.func(**_deserialize(arg_key))  # type: ignore
        duration = time.time() - start_time

        # Pop stack
        assert self.active_tasks[-1] == (self, arg_key), 'task stack is altered'
        self.active_tasks.pop()

        out = _Result(value=value, duration=duration)
        ts = self.db.save(key=arg_key, obj=out)
        return ts

    def _update(self, arg_key: Json) -> datetime:
        """ Update the cache and the dependencies recursively. """
        hit, miss = self.cache_stats.get(arg_key, (0, 0))

        try:
            # Check timestamp
            ts0 = self.db.load_timestamp(arg_key)
            upstream_tasks = self.db.load_deps(arg_key)
            upstream_timestamps = [fn._update(key) for fn, key in upstream_tasks]
            newer_upstream_tasks = [(fn, key, ts) for (fn, key), ts in zip(upstream_tasks, upstream_timestamps) if ts > ts0]
            if newer_upstream_tasks:
                LOGGER.warning(
                        f'Old cache detected in {self.name}: '
                        f'arguments={arg_key}, datetime={ts0}, '
                        f'newer_upstream_tasks={newer_upstream_tasks}'
                        )
                raise KeyError('Old cache detected')
            if DISABLE_CHECKPOINT_REUSE:
                raise KeyError('Reuse desabled')
        except KeyError as e:
            ts0 = self._run_and_save(arg_key)
            miss += 1
        else:
            hit += 1

        self.cache_stats[arg_key] = (hit, miss)
        return ts0

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """ Update cache and fetch the result. """
        # Serialize arguments
        arg_key = self._serialize_arguments(*args, **kwargs)

        # If there is an active downstream task, add this task to its dependencies
        if self.active_tasks:
            downstream_fn, downstream_key = self.active_tasks[-1]
            downstream_fn.db.add_dep(downstream_key, (self, arg_key))

        self._update(arg_key)
        result = self.db.load(arg_key)
        return result.value

    def get_result(self, *args: P.args, **kwargs: P.kwargs) -> _Result[T]:
        return self.db.load(self._serialize_arguments(*args, **kwargs))

    def list_cached_arguments(self) -> list[dict]:
        return list(map(_deserialize, self.db.list_keys()))

    def clear(self) -> None:
        try:
            self.db.clear()
        except:
            pass
        self.cache_stats = {}

    def delete(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self.db.delete(self._serialize_arguments(*args, **kwargs))


def _serialize(arguments: dict) -> Json:
    return Json(json.dumps(arguments))


def _deserialize(s: str) -> Any:
    return json.loads(s)
