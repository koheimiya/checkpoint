import inspect
from dataclasses import dataclass
from typing import Callable, ClassVar, Generic, TypeVar, ParamSpec, Any
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


@dataclass(frozen=True)
class _DB(Generic[T]):
    result_db: SqliteDict
    timestamp_db: SqliteDict

    @classmethod
    def make(cls, name: str, compress: bool) -> Self:
        path = str(CHECKPOINT_PATH / name) + ".sqlite"
        custom_encoder_decoder = {}
        if compress:
            custom_encoder_decoder['encode'] = _encode_compressed
            custom_encoder_decoder['decode'] = _decode_compressed
        return _DB(
                result_db=SqliteDict(path, tablename='result', **custom_encoder_decoder),
                timestamp_db=SqliteDict(path, tablename='timestamp')
                )

    def save(self, key: str, obj: T, timestamp: datetime) -> None:
        with self.result_db as resdb:
            resdb[key] = obj
            resdb.commit()
        with self.timestamp_db as tsdb:
            tsdb[key] = timestamp.timestamp()
            tsdb.commit()

    def load(self, key: str) -> T:
        with self.result_db as db:
            return db[key]

    def load_timestamp(self, key: str) -> datetime:
        with self.timestamp_db as db:
            return datetime.fromtimestamp(db[key])

    def list_keys(self) -> list[str]:
        with self.result_db as db:
            return list(db.keys())


def _encode_compressed(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))


def _decode_compressed(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))


@dataclass
class _Result(Generic[T]):
    value: T
    duration: float
    timestamp: datetime
    upstreams: list[tuple[_DB[Any], str]]


@dataclass
class _FunctionWithDB(Generic[P, T]):
    name: str
    func: Callable[P, T]
    db: _DB[_Result[T]]
    names: ClassVar[set[str]] = set()
    dependency_dict: ClassVar[dict[str, list[tuple[_DB, str]]]] = dict()

    def __post_init__(self) -> None:
        self.names.add(self.name)

    @classmethod
    def make(cls, name: str | None, func: Callable[P, T], compress: bool) -> Self:
        if name is None:
            name = f'{func.__module__}.{func.__name__}'
        if name in cls.names:
            raise ValueError(f'{name} already exists')

        db = _DB.make(name=name, compress=compress)
        return _FunctionWithDB(name=name, func=func, db=db)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        # Serialize arguments
        params = inspect.signature(self.func).bind(*args, **kwargs)
        params.apply_defaults()
        arguments = params.arguments
        arg_key = _serialize(arguments)

        try:
            # Try to fetch cache
            result = self.db.load(arg_key)
            ts0 = self.db.load_timestamp(arg_key)

            # Check timestamp
            newer_upstreams = {}
            for up_db, up_key in result.upstreams:
                up_ts = up_db.load_timestamp(up_key)
                if up_ts <= ts0:
                    continue
                newer_upstreams[up_db, up_key] = up_ts
            if newer_upstreams:
                LOGGER.warn(
                        f'Old cache detected in {self.name}: '
                        f'arguments={arg_key}, datetime={ts0}, '
                        f'newer_upstreams={newer_upstreams}'
                        )
                raise KeyError()
        except KeyError:
            # Set dependency detector
            task_key = _serialize({'name': self.name, 'arguments': arguments})
            if task_key in self.dependency_dict:
                raise RuntimeError(f'Loop detected at {task_key}')
            self.dependency_dict[task_key] = []

            # Run func
            start_time = time.time()
            value = self.func(*args, **kwargs)
            duration = time.time() - start_time
            timestamp = datetime.now()

            # Collect upstreams
            upstreams = self.dependency_dict.pop(task_key)

            # Save result
            result = _Result(
                    value=value,
                    duration=duration,
                    timestamp=timestamp,
                    upstreams=upstreams
                    )
            self.db.save(key=arg_key, obj=result, timestamp=timestamp)

        # Propagate dependencies to downstream tasks
        for upstreams in self.dependency_dict.values():
            upstreams.extend([(self.db, arg_key), *result.upstreams])

        return result.value

    def get_result(self, arguments: dict) -> _Result[T]:
        return self.db.load(_serialize(arguments))

    def list_cached_arguments(self) -> list[dict]:
        return list(map(_deserialize, self.db.list_keys()))


def _serialize(arguments: dict) -> str:
    return json.dumps(arguments)


def _deserialize(s: str) -> Any:
    return json.loads(s)


def checkpoint(name: str | None = None, compress: bool = False) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """ Decorator for persistent cache.
    The arguments of the function must be json-compatible.
    Values are stored in `$CHECKPOINT_PATH/checkpoints/{name}.sqlite`. `$CHECKPOINT_PATH` defaults to the current working directory.
    """
    def wrapper(func: Callable[P, T]) -> Callable[P, T]:
        return _FunctionWithDB.make(name=name, func=func, compress=compress)
    return wrapper
