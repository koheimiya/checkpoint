from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, Generic, NewType, Protocol, Self, Sequence, Type, TypeVar, Any, cast
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
P = ParamSpec('P')
R = TypeVar('R', covariant=True)
TaskReady = NewType('TaskReady', 'BaseTask')


class BaseTask(Generic[P, R], ABC):
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

    def get_prerequisite_tasks(self) -> Sequence[BaseTask[..., Any]]:
        cls = self.task__subclass
        inst = self.real_instance
        prerequisites: list[BaseTask[..., Any]] = []
        for _, v in inspect.getmembers(cls):
            if isinstance(v, Requires):
                prerequisites.append(getattr(inst, v.private_name))
            elif isinstance(v, RequiresList):
                prerequisites.extend(getattr(inst, v.private_name))
            elif isinstance(v, RequiresDict):
                prerequisites.extend(getattr(inst, v.private_name).values())
        assert all(isinstance(p, BaseTask) for p in prerequisites)
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
        if not any(isinstance(v, RequiresDirectory) for _, v in inspect.getmembers(self.task__subclass)):
            return
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir()
        setattr(self.real_instance, RequiresDirectory.private_name, self.directory)

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


# class TaskDefinitionProtocol(Protocol[P, R]):
#     def __init__(self, *args: P.args, **kwargs: P.kwargs) -> None: ...
#     def run(self) -> R: ...
# 
# 
# class TaskFactory(Generic[P, R]):
# 
#     def __init__(self, task_class: Type[TaskDefinitionProtocol[P, R]], compress_level: int, queue: str | None) -> None:
#         self.task_class = task_class
# 
#         self.name = _serialize_function(self.task_class)
#         self.db: Database[R] = Database.make(name=self.name, compress_level=compress_level)
#         self.queue = f'<{self.name}>' if queue is None else queue
# 
#         source = inspect.getsource(self.task_class)
#         formatted_source = ast.unparse(ast.parse(source))
#         self.source_timestamp = self.db.update_source_if_necessary(formatted_source)
# 
#         self.instance_registry: dict[Json, TaskDefinitionProtocol[P, R]] = {}
# 
#     def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Task[R]:
#         arg_key = _serialize_arguments(self.task_class, *args, **kwargs)
#         task_instance = self.instance_registry.get(arg_key, None)
#         if task_instance is None:
#             task_instance = self.task_class(*args, **kwargs)
#             self.instance_registry[arg_key] = task_instance
#         return Task(task_factory=self, arg_key=arg_key, task_instance=task_instance)


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
        if isinstance(o, BaseTask):
            return {'__task__': o.to_tuple()}
        else:
            # Let the base class default method raise the TypeError
            return super().default(o)


# def task(compress_level: int = 0, queue: str | None = None) -> Callable[[Type[TaskDefinitionProtocol[P, R]]], TaskFactory[P, R]]:
#     def decorator(cls: Type[TaskDefinitionProtocol[P, R]]) -> TaskFactory[P, R]:
#         return TaskFactory(cls, compress_level=compress_level, queue=queue)
#     return decorator


# @dataclass
# class Task(Generic[R]):
#     task_factory: TaskFactory[..., R]
#     arg_key: Json
#     task_instance: TaskDefinitionProtocol[..., R]
# 
#     @property
#     def _relative_path(self) -> str:
#         _, arg_str = self.to_tuple()
#         id_ = base64.urlsafe_b64encode(zlib.compress(arg_str.encode(), level=9)).decode().replace('=', '')
#         return os.path.join(*[id_[i:i+255] for i in range(0, len(id_), 255)])
# 
#     @property
#     def directory(self) -> Path:
#         return Path(self.task_factory.db.data_directory) / self._relative_path
# 
#     def clear(self) -> None:
#         db = self.task_factory.db
#         db.delete(self.arg_key)
#         if self.directory.exists():
#             shutil.rmtree(self.directory)
# 
#     @property
#     def queue(self) -> str:
#         return self.task_factory.queue
# 
#     @property
#     def source_timestamp(self) -> datetime:
#         return self.task_factory.source_timestamp
# 
#     def to_tuple(self) -> TaskKey:
#         return (self.task_factory.name, self.arg_key)
# 
#     def get_prerequisite_tasks(self) -> list[Self]:
#         cls = self.task_factory.task_class
#         inst = self.task_instance
#         requirement_names = [v.private_name for _, v in inspect.getmembers(cls) if isinstance(v, Requires)]
#         return [getattr(inst, k) for k in requirement_names]
# 
#     def peek_timestamp(self) -> datetime | None:
#         try:
#             return self.task_factory.db.load_timestamp(self.arg_key)
#         except KeyError:
#             return None
# 
#     def set_result(self) -> None:
#         db = self.task_factory.db
#         self._get_directory_prepared_if_necessary()
#         out = self.task_instance.run()
#         db.save(self.arg_key, out)
# 
#     def _get_directory_prepared_if_necessary(self) -> None:
#         if not any(isinstance(v, RequiresDirectory) for _, v in inspect.getmembers(self.task_factory.task_class)):
#             return
#         if self.directory.exists():
#             shutil.rmtree(self.directory)
#         self.directory.mkdir()
#         setattr(self.task_instance, RequiresDirectory.private_name, self.directory)
# 
#     def get_result(self) -> R:
#         return self.task_factory.db.load(self.arg_key)
# 
#     def run(
#             self, *,
#             executor: Executor | None = None,
#             max_workers: int | None = None,
#             rate_limits: dict[str, int] | None = None,
#             detect_source_change: bool | None = None,
#             ) -> R:
#         return self.run_with_info(
#                 executor=executor,
#                 max_workers=max_workers,
#                 rate_limits=rate_limits,
#                 detect_source_change=detect_source_change
#                 )[0]
# 
#     def run_with_info(
#             self, *,
#             executor: Executor | None = None,
#             max_workers: int | None = None,
#             rate_limits: dict[str, int] | None = None,
#             detect_source_change: bool | None = None,
#             dump_generations: bool = False
#             ) -> tuple[R, dict[str, Any]]:
#         if detect_source_change is None:
#             detect_source_change = Context.detect_source_change
#         graph = TaskGraph.build_from(self, detect_source_change=detect_source_change)
# 
#         if executor is None:
#             executor = Context.get_executor(max_workers=max_workers)
#         else:
#             assert max_workers is None
#         info = run_task_graph(graph=graph, executor=executor, rate_limits=rate_limits, dump_graphs=dump_generations)
#         return task.get_result(), info


class Req:
    def __set_name__(self, owner: BaseTask[..., Any], name: str) -> None:
        self.public_name = name
        self.private_name = '_' + name


class Requires(Req, Generic[T]):
    def __set__(self, obj: BaseTask[..., Any], value: BaseTask[..., T]) -> None:
        setattr(obj, self.private_name, value)

    def __get__(self, obj: BaseTask[..., Any], objtype=None) -> T:
        task = getattr(obj, self.private_name)
        return task.get_result()


class RequiresList(Req, Generic[T]):
    def __set__(self, obj: BaseTask[..., Any], value: list[BaseTask[..., T]]) -> None:
        setattr(obj, self.private_name, value)

    def __get__(self, obj: BaseTask[..., Any], objtype=None) -> list[T]:
        task_list = getattr(obj, self.private_name)
        return [task.get_result() for task in task_list]


class RequiresDict(Req, Generic[K, T]):
    def __set__(self, obj: BaseTask[..., Any], value: dict[K, BaseTask[..., T]]) -> None:
        setattr(obj, self.private_name, value)

    def __get__(self, obj: BaseTask[..., Any], objtype=None) -> dict[K, T]:
        task_dict = getattr(obj, self.private_name)
        return {k: task.get_result() for k, task in task_dict.items()}


class RequiresDirectory:
    private_name = '_task_directory'
    def __get__(self, obj: BaseTask[..., Any], objtype=None) -> Path:
        path = getattr(obj, self.private_name)
        assert isinstance(path, Path)
        return path


class Const(BaseTask[[T], T]):
    def init(self, x: T) -> None:
        self.x = x

    def main(self) -> T:
        return self.x
