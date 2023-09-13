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

from .types import JsonStr


T = TypeVar('T')


Serializer = tuple[Callable[[Any], bytes], Callable[[bytes], Any]]
DEFAULT_SERIALIZER: Serializer = (cloudpickle.dumps, cloudpickle.loads)
@dataclass(frozen=True)
class Database(Generic[T]):
    """ Manage the cache of tasks.
    Layout:
    base_path/
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
    base_path: Path
    id_table: IdTable
    serializer: Serializer = DEFAULT_SERIALIZER

    @classmethod
    def make(cls, cache_path: Path, name: str) -> Self:
        base_path = cache_path / name
        return Database(
                base_path=base_path,
                id_table=IdTable(base_path / 'id_table')
                )

    def __post_init__(self) -> None:
        self.results_directory.mkdir(exist_ok=True)

    def get_instance_dir(self, key: JsonStr, deps: dict[str, Path]) -> InstanceDirectory[T]:
        return InstanceDirectory(
                base_path=self.results_directory,
                instance_id=self.id_table.get(key),
                argkey=key,
                dependencies=deps,
                )

    @property
    def results_directory(self) -> Path:
        return Path(self.base_path) / 'results'

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

    def clear(self) -> None:
        if self.base_path.exists():
            shutil.rmtree(self.base_path)


def _get_timestamp(path: Path) -> datetime:
    timestamp = path.stat().st_mtime_ns / 10 ** 9
    return datetime.fromtimestamp(timestamp)


class InstanceDirectory(Generic[T]):
    def __init__(self, base_path: Path, instance_id: int, argkey: JsonStr, dependencies: dict[str, Path]):
        self.base_path = base_path
        self.task_id = instance_id
        self.argkey = argkey
        self.dependencies = dependencies
        if not self.path.exists():
            self.initialize()

    def initialize(self):
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True)
        with open(self.args_path, 'w') as ref:
            ref.write(self.argkey)
        self.data_dir.mkdir()
        self.deps_dir.mkdir()
        if self.dependencies:
            for name, target in self.dependencies.items():
                link_path = self.deps_dir / name
                link_path.symlink_to(target.resolve())
        else:
            (self.deps_dir / '__NO_DEPENDENCIES__').touch()

    @property
    def path(self):
        return self.base_path / str(self.task_id)

    @property
    def args_path(self) -> Path:
        return self.path / 'args.json'

    @property
    def result_path(self) -> Path:
        return self.path / f'result.pkl.gz'

    @property
    def stdout_path(self) -> Path:
        return self.path / f'stdout.txt'

    @property
    def stderr_path(self) -> Path:
        return self.path / f'stderr.txt'

    @property
    def data_dir(self) -> Path:
        return self.path / 'data'

    @property
    def deps_dir(self) -> Path:
        return self.path / 'deps'

    def save_result(self, obj: T, compress_level: int) -> datetime:
        path = self.result_path
        with gzip.open(path, 'wb', compresslevel=compress_level) as ref:
            cloudpickle.dump(obj, ref)
        return _get_timestamp(path)

    def load_result(self) -> T:
        path = self.result_path
        with gzip.open(path, 'rb') as ref:
            return cloudpickle.load(ref)

    def get_timestamp(self) -> datetime:
        path = self.result_path
        if self.result_path.exists():
            return _get_timestamp(path)
        else:
            raise RuntimeError(f'Result not found: {self.result_path}')

    def delete(self) -> None:
        self.initialize()


@dataclass
class IdTable:
    def __init__(self, path: Path | str) -> None:
        self.table = dc.Cache(directory=path)
        self.cache: dict[Any, int] = {}
    
    def get(self, x: Any) -> int:
        out = self.cache.get(x)
        if out is not None:
            return out

        with self.table.transact():
            value = self.table.get(key=x)
            if value is None:
                value = len(self.table)
                self.table.set(key=x, value=value)

        self.cache[x] = value
        return value

    def __contains__(self, key: Any) -> bool:
        return key in self.table

    def list_keys(self) -> list[str]:
        with self.table.transact():
            return list(map(str, self.table))

    def clear(self) -> None:
        self.table.clear()
