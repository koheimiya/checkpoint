from __future__ import annotations
from typing import Callable, Generic, TypeVar, Any
from typing_extensions import ParamSpec, Self
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil

import cloudpickle
import zlib
import diskcache as dc

from .types import Context
from .types import Json


K = TypeVar('K')
T = TypeVar('T')
S = TypeVar('S')
U = TypeVar('U')
P = ParamSpec('P')
Q = ParamSpec('Q')
R = TypeVar('R', covariant=True)


Serializer = tuple[Callable[[Any], bytes], Callable[[bytes], Any]]
DEFAULT_SERIALIZER: Serializer = (cloudpickle.dumps, cloudpickle.loads)


@dataclass(frozen=True)
class Database(Generic[T]):
    """ Manage the cache of tasks.
    Layout:
    Context.cache_dir / 'checkpoint' / name / result     # return values
    Context.cache_dir / 'checkpoint' / name / timestamp  # timestamps
    Context.cache_dir / 'checkpoint' / name / data       # other data created by tasks
    """
    name: str
    base_path: Path
    compress_level: int
    result_cache: dc.Cache
    timestamp_cache: dc.Cache
    serializer: Serializer = DEFAULT_SERIALIZER

    @classmethod
    def make(cls, name: str, compress_level: int) -> Self:
        base_path = Context.cache_dir / 'checkpoint' / name
        return Database(
                name=name,
                base_path=base_path,
                compress_level=compress_level,
                result_cache=dc.Cache(base_path / 'result'),
                timestamp_cache=dc.Cache(base_path / 'timestamp'),
                )

    def __post_init__(self) -> None:
        self.data_directory.mkdir(exist_ok=True)
        self.source_path.parent.mkdir(exist_ok=True)

    @property
    def data_directory(self) -> Path:
        return Path(self.base_path) / 'data'

    @property
    def source_path(self) -> Path:
        return Path(self.base_path) / 'code' / 'source.txt'

    def update_source_if_necessary(self, source: str) -> datetime:
        # Update source cache
        if self.source_path.exists():
            cached_source = open(self.source_path, 'r').read()
        else:
            cached_source = None
        if cached_source != source:
            open(self.source_path, 'w').write(source)
        return self.load_source_timestamp()

    def load_source_timestamp(self) -> datetime:
        timestamp = self.source_path.stat().st_mtime_ns / 10 ** 9
        return datetime.fromtimestamp(timestamp)

    def _dumps(self, obj: Any) -> bytes:
        dumps, _ = self.serializer
        return zlib.compress(dumps(obj), level=self.compress_level)

    def _loads(self, data: bytes) -> Any:
        _, loads = self.serializer
        return loads(zlib.decompress(data))

    def save(self, key: Json, obj: T) -> datetime:
        data = self._dumps(obj)
        with self.result_cache as ref:
            ref[key] = data

        timestamp = datetime.now()
        with self.timestamp_cache as ref:
            ref[key] = timestamp.timestamp()
        return timestamp

    def load(self, key: Json) -> T:
        with self.result_cache as ref:
            data = ref[key]
        return self._loads(data)

    def load_timestamp(self, key: Json) -> datetime:
        with self.timestamp_cache as ref:
            ts = ref[key]
        return datetime.fromtimestamp(ts)

    def __contains__(self, key: T) -> bool:
        with self.result_cache as ref:
            return key in ref

    def list_keys(self) -> list[str]:
        with self.result_cache as ref:
            return list(map(str, ref))

    def _get_caches(self) -> list[dc.Cache]:
        return [self.result_cache, self.timestamp_cache]

    def clear(self) -> None:
        for cache in self._get_caches():
            cache.clear()
        if self.data_directory.exists():
            shutil.rmtree(self.data_directory)
        self.data_directory.mkdir()

    def delete(self, key: Json) -> None:
        for cache in self._get_caches():
            with cache as ref:
                del ref[key]

