from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, NewType, TypeVar, ParamSpec, Any
from datetime import datetime
import os
from pathlib import Path
from typing_extensions import Self
import zlib
import dill
import sqlite3
from sqlitedict import SqliteDict


from .env import CHECKPOINT_PATH
from .base_db import Json


T = TypeVar('T')
D = TypeVar('D')


@dataclass
class SqliteDictDB(Generic[T, D]):
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
        return SqliteDictDB(
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
        if not isinstance(other, SqliteDictDB):
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

    def add_dep(self, key: Json, dep: D) -> None:
        with self.dependency_dict as d:
            try:
                deps = d[key] + [dep]
            except KeyError:
                deps = [dep]
            d[key] = deps
            d.commit()

    def reset_deps(self, key: Json) -> None:
        with self.dependency_dict as d:
            d[key] = []
            d.commit()

    def load(self, key: Json) -> T:
        with self.result_dict as d:
            return d[key]

    def load_timestamp(self, key: Json) -> datetime:
        with self.timestamp_dict as d:
            return datetime.fromtimestamp(d[key])

    def load_deps(self, key: Json) -> list[D]:
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
