from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, Generic, Protocol, Self, Sequence, Type, TypeVar, Any, cast, overload
from typing_extensions import ParamSpec
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from concurrent.futures import Executor
import ast
import os
import logging
import inspect
import json
import base64
import shutil

import zlib

from .types import Json, TaskKey, Context
from .database import Database
from .graph import TaskGraph, run_task_graph


LOGGER = logging.getLogger(__name__)


K = TypeVar('K')
T = TypeVar('T')
U = TypeVar('U')
P = ParamSpec('P')
R = TypeVar('R', covariant=True)


class Task(Generic[P, R], ABC):
    task__subclass: Type[Self]
    task__name: str
    task__db: Database[R]
    task__instance_registry: dict[Json, Self]
    task__queue: str
    task__source_timestamp: datetime
    task__compress_level: int = 0

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.task__subclass = cls
        cls.task__name = _serialize_function(cls)
        cls.task__db = Database.make(name=cls.task__name, compress_level=cls.task__compress_level)
        cls.task__queue = f'<{cls.task__name}>'
        cls.task__instance_registry = {}

        source = inspect.getsource(cls)
        formatted_source = ast.unparse(ast.parse(source))
        cls.task__source_timestamp = cls.task__db.update_source_if_necessary(formatted_source)

    def __init__(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self.arg_key = _serialize_arguments(self.task__subclass, *args, **kwargs)
        task_instance = self.task__instance_registry.get(self.arg_key, None)
        if task_instance is None:
            self.init(*args, **kwargs)
            self.real_instance = self.task__instance_registry[self.arg_key] = self
        else:
            self.real_instance = task_instance

    @abstractmethod
    def init(self, *args: P.args, **kwargs: P.kwargs) -> None: ...

    @abstractmethod
    def main(self) -> R: ...

    def to_tuple(self) -> TaskKey:
        return (self.task__name, self.arg_key)

    @property
    def _relative_path(self) -> str:
        _, arg_str = self.to_tuple()
        id_ = base64.urlsafe_b64encode(zlib.compress(arg_str.encode(), level=9)).decode().replace('=', '')
        return os.path.join(*[id_[i:i+255] for i in range(0, len(id_), 255)])

    @property
    def directory(self) -> Path:
        return Path(self.task__db.data_directory) / self._relative_path

    @classmethod
    def clear_all(cls) -> None:
        db = cls.task__db
        db.clear()

    def clear(self) -> None:
        db = self.task__db
        db.delete(self.arg_key)
        if self.directory.exists():
            shutil.rmtree(self.directory)

    @property
    def queue(self) -> str:
        return self.task__queue

    @property
    def source_timestamp(self) -> datetime:
        return self.task__source_timestamp

    def get_prerequisite_tasks(self) -> Sequence[Task[..., Any]]:
        cls = self.task__subclass
        inst = self.real_instance
        prerequisites: list[Task[..., Any]] = []
        for _, v in inspect.getmembers(cls):
            if isinstance(v, Req):
                prerequisites.extend(v.get_task_list(inst))
        assert all(isinstance(p, Task) for p in prerequisites)
        return prerequisites

    def peek_timestamp(self) -> datetime | None:
        try:
            return self.task__db.load_timestamp(self.arg_key)
        except KeyError:
            return None

    def set_result(self) -> None:
        db = self.task__db
        self._get_directory_prepared_if_necessary()
        out = self.real_instance.main()
        db.save(self.arg_key, out)

    def _get_directory_prepared_if_necessary(self) -> None:
        for _, v in inspect.getmembers(self.task__subclass):
            if isinstance(v, Directory):
                v.path = self.directory

        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir()

    def get_result(self) -> R:
        return self.task__db.load(self.arg_key)

    def run(
            self, *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            ) -> R:
        return self.run_with_info(
                executor=executor,
                max_workers=max_workers,
                rate_limits=rate_limits,
                detect_source_change=detect_source_change
                )[0]

    def run_with_info(
            self, *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            dump_generations: bool = False
            ) -> tuple[R, dict[str, Any]]:
        if detect_source_change is None:
            detect_source_change = Context.detect_source_change
        graph = TaskGraph.build_from(self, detect_source_change=detect_source_change)

        if executor is None:
            executor = Context.get_executor(max_workers=max_workers)
        else:
            assert max_workers is None
        info = run_task_graph(graph=graph, executor=executor, rate_limits=rate_limits, dump_graphs=dump_generations)
        return self.get_result(), info


class TaskTypeProtocol(Protocol[P, R]):
    def init(self, *args: P.args, **kwargs: P.kwargs) -> None: ...
    def main(self) -> R: ...


def infer_task_type(cls: Type[TaskTypeProtocol[P, R]]) -> Callable[P, Task[P, R]]:
    assert issubclass(cls, Task), f'{cls} must inherit from {Task} to infer task type.'
    return cast(Callable[P, Task[P, R]], cls)


def _serialize_function(fn: Callable[..., Any]) -> str:
    return f'{fn.__module__}.{fn.__qualname__}'


def _normalize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
    params = inspect.signature(fn).bind(*args, **kwargs)
    params.apply_defaults()
    return params.arguments


def _serialize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> Json:
    arguments = _normalize_arguments(fn, *args, **kwargs)
    return cast(Json, json.dumps(arguments, separators=(',', ':'), sort_keys=True, cls=CustomJSONEncoder))


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Task):
            return {'__task__': o.to_tuple()}
        else:
            # Let the base class default method raise the TypeError
            return super().default(o)


class Req(Generic[T, R]):
    def __set_name__(self, _: Task[..., Any], name: str) -> None:
        self.public_name = name
        self.private_name = '_requires__' + name

    def __set__(self, obj: Task[..., Any], value: T) -> None:
        setattr(obj, self.private_name, value)

    @overload
    def __get__(self: Req[TaskLike[U], U], obj: Task[..., Any], _=None) -> U: ...
    @overload
    def __get__(self: Req[list[TaskLike[U]], list[U]], obj: Task[..., Any], _=None) -> list[U]: ...
    @overload
    def __get__(self: Req[dict[K, TaskLike[U]], dict[K, U]], obj: Task[..., Any], _=None) -> dict[K, U]: ...
    @overload
    def __get__(self: Req[Directory, Path], obj: Task[..., Any], _=None) -> Path: ...
    def __get__(self, obj: Task[..., Any], _=None) -> Any:
        task = getattr(obj, self.private_name)
        if isinstance(task, Task):
            return task.get_result()
        elif isinstance(task, list):
            return [t.get_result() for t in task]
        elif isinstance(task, dict):
            return {k: v.get_result() for k, v in task.items()}
        elif isinstance(task, Directory):
            return task.get_path_prepared()
        elif isinstance(task, Const):
            return task.value
        else:
            raise TypeError(f'Unsupported requirement type: {type(task)}')

    def get_task_list(self, obj: Task[..., Any]) -> list[Task[..., Any]]:
        task = getattr(obj, self.private_name)
        if isinstance(task, Task):
            return [task]
        elif isinstance(task, list):
            return task
        elif isinstance(task, dict):
            return [v.get_result() for _, v in task.items()]
        elif isinstance(task, Directory):
            return []
        elif isinstance(task, Const):
            return []
        else:
            raise TypeError(f'Unsupported requirement type: {type(task)}')


@dataclass
class Directory:
    path: Path | None = None

    def get_path_prepared(self) -> Path:
        assert self.path is not None, 'Should never happen.'
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir()
        return self.path


@dataclass
class Const(Generic[T]):
    value: T

    def get_result(self) -> T:
        return self.value


TaskLike = Task[..., U] | Const[U]


Requires = Req[TaskLike[U], U]
RequiresList = Req[list[TaskLike[U]], list[U]]
RequiresDict = Req[dict[K, TaskLike[U]], dict[K, U]]
RequiresDirectory = Req[Directory, Path]
