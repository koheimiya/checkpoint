from __future__ import annotations
import inspect
from functools import wraps
from dataclasses import dataclass, field
from typing import Callable, ClassVar, Generic, NewType, TypeVar, ParamSpec, Any
import logging
import time
from datetime import datetime
import json
import os
from pathlib import Path
from typing_extensions import Self
import zlib
import dill
import sqlite3
from sqlitedict import SqliteDict


LOGGER = logging.getLogger(__name__)


CHECKPOINT_PATH = Path(os.getenv('CHECKPOINT_DIR', './')) / '.checkpoints'
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)


DISABLE_CHECKPOINT_REUSE = bool(os.getenv('DISABLE_CHECKPOINT', 0))


T = TypeVar('T')
P = ParamSpec('P')


Json = NewType('Json', str)


@dataclass
class _DB(Generic[T]):
    """ Key-value store with JSON keys and python object values.
    Automatically log timestamp.
    """
    path: str
    compress: bool
    result_dict: SqliteDict
    timestamp_dict: SqliteDict
    dependency_dict: SqliteDict

    @classmethod
    def make(cls, name: str, compress: bool) -> Self:
        path = str(CHECKPOINT_PATH / name) + ".sqlite"
        return cls._make(path, compress)

    @classmethod
    def _make(cls, path: str, compress: bool) -> Self:
        converter = {'encode': _encode, 'decode': _decode}
        converter_with_compression = {'encode': _encode_and_compress, 'decode': _decompress_and_decode}
        result_converter = converter_with_compression if compress else converter
        return _DB(
                path=path,
                compress=compress,
                result_dict=SqliteDict(path, tablename='result', **result_converter),
                timestamp_dict=SqliteDict(path, tablename='timestamp', **converter),
                dependency_dict=SqliteDict(path, tablename='dependency', **converter),
                )

    def __getstate__(self) -> dict:
        return {'path': self.path, 'compress': self.compress}

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        db = self._make(self.path, self.compress)
        self.result_dict = db.result_dict
        self.timestamp_dict = db.timestamp_dict
        self.dependency_dict = db.dependency_dict

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _DB):
            return False
        return (self.path == other.path) and (self.compress == other.compress)

    def save(self, key: Json, obj: T) -> datetime:
        with self.result_dict as d:
            d[key] = obj
            d.commit()

        timestamp = datetime.now()
        with self.timestamp_dict as d:
            d[key] = timestamp.timestamp()
            d.commit()
        return timestamp

    def add_dependency(self, key: Json, dep: tuple[_FunctionWithDB, Json]) -> None:
        with self.dependency_dict as d:
            try:
                deps = d[key] + [dep]
            except KeyError:
                deps = [dep]
            d[key] = deps
            d.commit()

    def reset_dependencies(self, key: Json) -> None:
        with self.dependency_dict as d:
            d[key] = []
            d.commit()

    def load(self, key: Json) -> T:
        with self.result_dict as d:
            return d[key]

    def load_timestamp(self, key: Json) -> datetime:
        with self.timestamp_dict as d:
            return datetime.fromtimestamp(d[key])

    def load_deps(self, key: Json) -> list[tuple[_FunctionWithDB, Json]]:
        with self.dependency_dict as d:
            return d[key]

    def list_keys(self) -> list[Json]:
        with self.result_dict as d:
            return list(d.keys())

    def _get_dicts(self) -> list[SqliteDict]:
        return [self.result_dict, self.timestamp_dict, self.dependency_dict]

    def clear(self) -> None:
        for d in self._get_dicts():
            try:
                d.clear()
            except:
                pass

    def delete(self, key: Json) -> None:
        for dct in self._get_dicts():
            with dct as d:
                del d[key]
                d.commit()


def _encode(obj: Any) -> sqlite3.Binary:
    return sqlite3.Binary(dill.dumps(obj, dill.HIGHEST_PROTOCOL))


def _decode(obj: Any) -> Any:
    return dill.loads(bytes(obj))


def _encode_and_compress(obj: Any) -> sqlite3.Binary:
    return sqlite3.Binary(zlib.compress(dill.dumps(obj, dill.HIGHEST_PROTOCOL)))


def _decompress_and_decode(obj: Any) -> Any:
    return dill.loads(zlib.decompress(bytes(obj)))


@dataclass
class _Result(Generic[T]):
    value: T
    duration: float


@dataclass
class _FunctionWithDB(Generic[P, T]):
    name: str
    func: Callable[P, T]
    db: _DB[_Result[T]]
    cache_stats: dict[Json, tuple[int, int]] = field(default_factory=dict)  # hit and miss

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

    def _run_and_save(self, arg_key: Json) -> datetime:
        """ Actually call the function with serialized arguments """
        # Push current task to stack and reset dependencies
        self.active_tasks.append((self, arg_key))
        self.db.reset_dependencies(arg_key)

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
            downstream_fn.db.add_dependency(downstream_key, (self, arg_key))

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


@dataclass
class checkpoint:
    """ Decorator for persistent cache.
    The arguments of the function must be json-compatible.
    Values are stored in `$CHECKPOINT_PATH/.checkpoints/{name}.sqlite`. `$CHECKPOINT_PATH` defaults to the current working directory.
    """
    name: str | None = None
    compress: bool = False

    def __call__(self, func: Callable[P, T]) -> _FunctionWithDB[P, T]:
        return wraps(func)(_FunctionWithDB.make(name=self.name, func=func, compress=self.compress))
