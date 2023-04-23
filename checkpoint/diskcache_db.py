from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, NewType, TypeVar
from typing_extensions import Self
from datetime import datetime

import diskcache as dc
import dill
import zlib


from .env import CHECKPOINT_PATH
from .base_db import Json


T = TypeVar('T')
D = TypeVar('D')


@dataclass
class DiskCacheDB(Generic[T, D]):
    path: str
    compress_level: int | None
    result_cache: dc.Cache
    timestamp_cache: dc.Cache
    dependency_cache: dc.Cache

    @classmethod
    def make(cls, name: str, compress_level: int | None) -> Self:
        path = str(CHECKPOINT_PATH / name)
        disk_args_compress = {
                'disk': DillDisk,
                'disk_compress_level': compress_level
                }
        disk_args = {
                'disk': DillDisk,
                'disk_compress_level': None
                }
        return DiskCacheDB(
                path=path,
                compress_level=compress_level,
                result_cache=dc.Cache(path + '/result', **disk_args_compress),
                timestamp_cache=dc.Cache(path + '/timestamp', **disk_args),
                dependency_cache=dc.Cache(path + '/deps', **disk_args),
                )

    def save(self, key: Json, obj: T) -> datetime:
        with self.result_cache as ref:
            ref[key] = obj

        timestamp = datetime.now()
        with self.timestamp_cache as ref:
            ref[key] = timestamp.timestamp()
        return timestamp

    def add_dep(self, key: Json, dep: D) -> None:
        with self.dependency_cache as ref:
            try:
                deps = ref[key] + [dep]
            except KeyError:
                deps = [dep]
            ref[key] = deps

    def reset_deps(self, key: Json) -> None:
        with self.dependency_cache as ref:
            ref[key] = []

    def load(self, key: Json) -> T:
        with self.result_cache as ref:
            return ref[key]

    def load_timestamp(self, key: Json) -> datetime:
        with self.timestamp_cache as ref:
            return datetime.fromtimestamp(ref[key])

    def load_deps(self, key: Json) -> list[D]:
        with self.dependency_cache as ref:
            return ref[key]

    def list_keys(self) -> list[str]:
        with self.result_cache as ref:
            return list(map(str, ref))

    def _get_caches(self) -> list[dc.Cache]:
        return [self.result_cache, self.timestamp_cache, self.dependency_cache]

    def clear(self) -> None:
        for cache in self._get_caches():
            cache.clear()

    def delete(self, key: Json) -> None:
        for cache in self._get_caches():
            with cache as ref:
                del ref[key]


class DillDisk(dc.Disk):
    def __init__(self, directory: str, compress_level: int | None, **kwargs):
        self.compress_level = compress_level
        super().__init__(directory, **kwargs)

    def put(self, key):
        data = dill.dumps(key, byref=True)
        if self.compress_level is not None:
            data = zlib.compress(data, self.compress_level)
        return super().put(data)

    def get(self, key, raw):
        data = super().get(key, raw)
        if self.compress_level is not None:
            data = zlib.decompress(data)
        return dill.loads(data)

    def store(self, value, read, key=dc.UNKNOWN):
        if not read:
            value = dill.dumps(value, byref=True)
            if self.compress_level is not None:
                value = zlib.compress(value, self.compress_level)
        return super().store(value, read, key=key)

    def fetch(self, mode, filename, value, read):
        data = super().fetch(mode, filename, value, read)
        if not read:
            if self.compress_level is not None:
                data = zlib.decompress(data)
            data = dill.loads(data)
        return data
