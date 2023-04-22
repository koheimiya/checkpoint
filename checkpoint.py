from __future__ import annotations
from functools import wraps
import inspect
from dataclasses import dataclass
from typing import Callable, ClassVar, Generic, NewType, TypeVar, ParamSpec, Any
import logging
import time
from datetime import datetime
import json
import os
from pathlib import Path
from typing_extensions import Self
import zlib
import pickle
import sqlite3
from sqlitedict import SqliteDict


LOGGER = logging.getLogger(__name__)


CHECKPOINT_PATH = Path(os.getenv('CHECKPOINT_DIR', './')) / 'checkpoints'
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)


T = TypeVar('T')
P = ParamSpec('P')


Json = NewType('Json', str)


@dataclass
class _DB(Generic[T]):
    result_dict: SqliteDict
    timestamp_dict: SqliteDict
    dependency_dict: SqliteDict

    @classmethod
    def make(cls, name: str, compress: bool) -> Self:
        path = str(CHECKPOINT_PATH / name) + ".sqlite"
        custom_encoder_decoder = {}
        if compress:
            custom_encoder_decoder['encode'] = _encode_and_compress
            custom_encoder_decoder['decode'] = _decompress_and_decode
        return _DB(
                result_dict=SqliteDict(path, tablename='result', **custom_encoder_decoder),
                timestamp_dict=SqliteDict(path, tablename='timestamp'),
                dependency_dict=SqliteDict(path, tablename='dependency'),
                )

    def save(self, key: Json, obj: T) -> datetime:
        with self.result_dict as res_dict:
            res_dict[key] = obj
            res_dict.commit()

        timestamp = datetime.now()
        with self.timestamp_dict as ts_dict:
            ts_dict[key] = timestamp.timestamp()
            ts_dict.commit()
        return timestamp

    def add_dependency(self, key: Json, dep: tuple[_FunctionWithDB, Json]) -> None:
        with self.dependency_dict as deps_dict:
            try:
                deps = deps_dict[key] + [dep]
            except KeyError:
                deps = [dep]
            deps_dict[key] = deps
            deps_dict.commit()

    def reset_dependencies(self, key: Json) -> None:
        with self.dependency_dict as deps_dict:
            deps_dict[key] = []
            deps_dict.commit()

    def load(self, key: Json) -> T:
        with self.result_dict as db:
            return db[key]

    def load_timestamp(self, key: Json) -> datetime:
        with self.timestamp_dict as db:
            return datetime.fromtimestamp(db[key])

    def load_deps(self, key: Json) -> list[tuple[_FunctionWithDB, Json]]:
        with self.dependency_dict as db:
            return db[key]

    def list_keys(self) -> list[Json]:
        with self.result_dict as db:
            return list(db.keys())


def _encode_and_compress(obj: Any) -> sqlite3.Binary:
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))


def _decompress_and_decode(obj: Any) -> Any:
    return pickle.loads(zlib.decompress(bytes(obj)))


@dataclass
class _Result(Generic[T]):
    value: T
    duration: float


@dataclass
class _FunctionWithDB(Generic[P, T]):
    name: str
    func: Callable[P, T]
    db: _DB[_Result[T]]
    cache_stats: dict[Json, tuple[int, int]] = {}  # hit and miss

    name_register: ClassVar[set[str]] = set()
    active_tasks: ClassVar[list[tuple[_FunctionWithDB, Json]]] = []

    def __post_init__(self) -> None:
        self.name_register.add(self.name)

    @classmethod
    def make(cls, name: str | None, func: Callable[P, T], compress: bool) -> Self:
        if name is None:
            name = f'{func.__module__}.{func.__name__}'
        if name in cls.name_register:
            raise ValueError(f'{name} already exists')

        db = _DB.make(name=name, compress=compress)
        return _FunctionWithDB(name=name, func=func, db=db)

    def _serialize_arguments(self, *args: P.args, **kwargs: P.kwargs) -> Json:
        params = inspect.signature(self.func).bind(*args, **kwargs)
        params.apply_defaults()
        arguments = params.arguments
        return _serialize(arguments)

    def _run(self, arg_key: Json) -> _Result[T]:
        """ Actually call the function with serialized arguments """
        # Push current task to stack and reset dependencies
        self.active_tasks.append((self, arg_key))
        self.db.reset_dependencies(arg_key)

        # Run func
        start_time = time.time()
        value = self.func(**_deserialize(arg_key))  # type: ignore
        duration = time.time() - start_time

        # Pop stack
        assert self.active_tasks[-1] is self, 'task stack is altered'
        self.active_tasks.pop()

        out = _Result(value=value, duration=duration)
        return out

    def _update(self, arg_key: Json) -> datetime:
        """ Update the cache and the dependencies recursively. """
        hit, miss = self.cache_stats.get(arg_key, (0, 0))

        try:
            # Check timestamp
            ts0 = self.db.load_timestamp(arg_key)
            upstream_tasks = self.db.load_deps(arg_key)
            upstream_timestamps = [fn._update(key) for fn, key in upstream_tasks]
            newer_upstream_tasks = [(fn, key, ts) for (fn, key), ts in zip(upstream_tasks, upstream_timestamps)]
            if newer_upstream_tasks:
                LOGGER.warn(
                        f'Old cache detected in {self.name}: '
                        f'arguments={arg_key}, datetime={ts0}, '
                        f'newer_upstream_tasks={newer_upstream_tasks}'
                        )
                raise KeyError()
        except KeyError:
            result = self._run(arg_key)
            ts0 = self.db.save(key=arg_key, obj=result)
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
            downstream_fn.db.add_dependency(downstream_key, (self, arg_key))

        self._update(arg_key)
        result = self.db.load(arg_key)
        return result.value

    def get_result(self, *args: P.args, **kwargs: P.kwargs) -> _Result[T]:
        return self.db.load(self._serialize_arguments(*args, **kwargs))

    def list_cached_arguments(self) -> list[dict]:
        return list(map(_deserialize, self.db.list_keys()))


def _serialize(arguments: dict) -> Json:
    return Json(json.dumps(arguments))


def _deserialize(s: str) -> Any:
    return json.loads(s)


def checkpoint(name: str | None = None, compress: bool = False) -> Callable[[Callable[P, T]], _FunctionWithDB[P, T]]:
    """ Decorator for persistent cache.
    The arguments of the function must be json-compatible.
    Values are stored in `$CHECKPOINT_PATH/checkpoints/{name}.sqlite`. `$CHECKPOINT_PATH` defaults to the current working directory.
    """
    def wrapper(func: Callable[P, T]) -> _FunctionWithDB[P, T]:
        return _FunctionWithDB.make(name=name, func=func, compress=compress)
    return wrapper
