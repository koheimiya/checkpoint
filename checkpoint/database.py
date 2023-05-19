from __future__ import annotations
from typing import Callable, Generic, TypeVar, Any
from typing_extensions import Self
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil
import gzip

import cloudpickle
import diskcache as dc

from .types import Context
from .types import Json


T = TypeVar('T')


Serializer = tuple[Callable[[Any], bytes], Callable[[bytes], Any]]
DEFAULT_SERIALIZER: Serializer = (cloudpickle.dumps, cloudpickle.loads)
@dataclass(frozen=True)
class Database(Generic[T]):
    """ Manage the cache of tasks.
    Layout:
    Context.cache_dir/'checkpoint'/name/
        * source.txt
        * id_table
        * results/
            * 0/
                * args.json
                * result.pkl.gz
                * stdout.txt
                * stderr.txt
                * data/
            * 1/
                * args.json
                * result.pkl.gz
                * stdout.txt
                * stderr.txt
                * data/
            ...
    """
    name: str
    base_path: Path
    compress_level: int
    id_table: IdTable
    serializer: Serializer = DEFAULT_SERIALIZER

    @classmethod
    def make(cls, name: str, compress_level: int) -> Self:
        base_path = Context.cache_dir / 'checkpoint' / name
        return Database(
                name=name,
                base_path=base_path,
                compress_level=compress_level,
                id_table=IdTable(base_path / 'id_table')
                )

    def __post_init__(self) -> None:
        self.results_directory.mkdir(exist_ok=True)

    @property
    def results_directory(self) -> Path:
        return Path(self.base_path) / 'results'

    def get_result_dir(self, key: Json) -> Path:
        taskid = self.id_table.get(key)
        out = self.results_directory / f'{taskid}'
        if not out.exists():
            out.mkdir()
            with open(out / 'args.json', 'w') as ref:
                ref.write(key)
        return out

    def get_result_path(self, key: Json) -> Path:
        return self.get_result_dir(key) / f'result.pkl.gz'

    def get_stdout_path(self, key: Json) -> Path:
        return self.get_result_dir(key) / f'stdout.txt'

    def get_stderr_path(self, key: Json) -> Path:
        return self.get_result_dir(key) / f'stderr.txt'

    def get_data_dir(self, key: Json) -> Path:
        return self.get_result_dir(key) / 'data'

    @property
    def source_path(self) -> Path:
        return Path(self.base_path) / 'source.txt'

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
        return _get_timestamp(self.source_path)

    def save(self, key: Json, obj: T) -> datetime:
        path = self.get_result_path(key)
        with gzip.open(path, 'wb', compresslevel=self.compress_level) as ref:
            cloudpickle.dump(obj, ref)
        return _get_timestamp(path)

    def load(self, key: Json) -> T:
        path = self.get_result_path(key)
        with gzip.open(path, 'rb') as ref:
            return cloudpickle.load(ref)

    def load_timestamp(self, key: Json) -> datetime:
        path = self.get_result_path(key)
        if path.exists():
            return _get_timestamp(path)
        else:
            raise KeyError(key)

    def clear(self) -> None:
        self.id_table.clear()
        if self.results_directory.exists():
            shutil.rmtree(self.results_directory)
        self.results_directory.mkdir()

    def delete(self, key: Json) -> None:
        resdir = self.get_result_path(key)
        if resdir.exists():
            resdir.unlink()
        else:
            raise KeyError(key)
        datadir = self.get_data_dir(key)
        if datadir.exists():
            shutil.rmtree(datadir)


def _get_timestamp(path: Path) -> datetime:
    timestamp = path.stat().st_mtime_ns / 10 ** 9
    return datetime.fromtimestamp(timestamp)


@dataclass
class IdTable:
    def __init__(self, path: Path | str) -> None:
        self.table = dc.Cache(directory=path)
    
    def get(self, x: Any) -> int:
        with self.table as ref:
            try:
                return ref[x]
            except KeyError:
                n = len(ref)
                ref[x] = n
                return n

    def __contains__(self, key: Any) -> bool:
        with self.table as ref:
            return key in ref

    def list_keys(self) -> list[str]:
        with self.table as ref:
            return list(map(str, ref))

    def clear(self) -> None:
        self.table.clear()
