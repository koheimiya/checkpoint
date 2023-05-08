from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, ClassVar, Generic, Mapping, Protocol, Self, Sequence, Type, TypeVar, Any, cast, overload, get_origin
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


class TaskConfig(Generic[P, R]):
    """ Information specific to a task class (not instance) """
    def __init__(
            self,
            task_class: Type[Task[P, R]],
            queue: str | None,
            compress_level: int,
            ) -> None:

        self.task_class = task_class
        self.name = _serialize_function(task_class)
        self.db = Database.make(name=self.name, compress_level=compress_level)
        self.queue = queue if queue is not None else f'<{self.name}>'
        self.worker_registry: dict[Json, TaskWorker[R]] = {}

        source = inspect.getsource(task_class)
        formatted_source = ast.unparse(ast.parse(source))
        self.source_timestamp = self.db.update_source_if_necessary(formatted_source)

    @property
    def data_directory(self) -> Path:
        return self.db.data_directory

    def clear_all(self) -> None:
        self.db.clear()


class TaskWorker(Generic[R]):
    @classmethod
    def make(cls, config: TaskConfig[P, R], instance: Task[P, R], *args: P.args, **kwargs: P.kwargs) -> Self:
        arg_key = _serialize_arguments(instance.init, *args, **kwargs)
        worker = config.worker_registry.get(arg_key, None)
        if worker is not None:
            return worker

        instance.init(*args, **kwargs)
        worker = TaskWorker[R](config=config, instance=instance, arg_key=arg_key)
        config.worker_registry[arg_key] = worker
        return worker

    def __init__(self, config: TaskConfig[..., R], instance: Task[..., R], arg_key: Json) -> None:
        self.config = config
        self.instance = instance
        self.arg_key = arg_key

    @property
    def queue(self) -> str:
        return self.config.queue

    @property
    def source_timestamp(self) -> datetime:
        return self.config.source_timestamp

    def to_tuple(self) -> TaskKey:
        return (self.config.name, self.arg_key)

    def get_prerequisites(self) -> Sequence[TaskWorker[Any]]:
        cls = self.config.task_class
        inst = self.instance
        prerequisites: list[TaskWorker[Any]] = []
        for _, v in inspect.getmembers(cls):
            if isinstance(v, Req):
                prerequisites.extend([task.task_worker for task in v.get_task_list(inst)])
        assert all(isinstance(p, TaskWorker) for p in prerequisites)
        return prerequisites

    def peek_timestamp(self) -> datetime | None:
        try:
            return self.config.db.load_timestamp(self.arg_key)
        except KeyError:
            return None

    def set_result(self) -> None:
        db = self.config.db
        if self.directory.exists():
            shutil.rmtree(self.directory)
        out = self.instance.main()
        db.save(self.arg_key, out)

    @property
    def _relative_path(self) -> str:
        _, arg_str = self.to_tuple()
        id_ = base64.urlsafe_b64encode(zlib.compress(arg_str.encode(), level=9)).decode().replace('=', '')
        return os.path.join(*[id_[i:i+255] for i in range(0, len(id_), 255)])

    @property
    def _directory_uninit(self) -> Path:
        return Path(self.config.data_directory) / self._relative_path

    @property
    def directory(self) -> Path:
        out = self._directory_uninit
        out.mkdir(exist_ok=True)
        return out

    def get_result(self) -> R:
        return self.config.db.load(self.arg_key)

    def clear(self) -> None:
        db = self.config.db
        db.delete(self.arg_key)
        directory = self._directory_uninit
        if directory.exists():
            shutil.rmtree(directory)


class Task(Generic[P, R], ABC):
    task_config: TaskConfig[P, R]

    @abstractmethod
    def init(self, *args: P.args, **kwargs: P.kwargs) -> None:
        pass

    @abstractmethod
    def main(self) -> R:
        pass

    def __init_subclass__(cls, **kwargs: Any) -> None:
        queue = kwargs.pop('queue', None)
        compress_level = kwargs.pop('compress_level', 0)

        # Fill missing requirement
        ann = inspect.get_annotations(cls, eval_str=True)
        for k, v in ann.items():
            if get_origin(v) is Req and getattr(cls, k, None) is None:
                req = Req()
                req.__set_name__(None, k)
                setattr(cls, k, req)

        cls.task_config = TaskConfig(task_class=cls, queue=queue, compress_level=compress_level)
        super().__init_subclass__(**kwargs)

    def __init__(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self.task_worker: TaskWorker[R] = TaskWorker.make(
                self.task_config, self, *args, **kwargs
                )

    @classmethod
    def clear_all_tasks(cls) -> None:
        cls.task_config.clear_all()

    def clear_task(self) -> None:
        self.task_worker.clear()

    def run_task(
            self, *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            ) -> R:
        return self.run_task_with_stats(
                executor=executor,
                max_workers=max_workers,
                rate_limits=rate_limits,
                detect_source_change=detect_source_change
                )[0]

    def run_task_with_stats(
            self, *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            dump_generations: bool = False
            ) -> tuple[R, dict[str, Any]]:
        if detect_source_change is None:
            detect_source_change = Context.detect_source_change
        graph = TaskGraph.build_from(self.task_worker, detect_source_change=detect_source_change)

        if executor is None:
            executor = Context.get_executor(max_workers=max_workers)
        else:
            assert max_workers is None
        stats = run_task_graph(graph=graph, executor=executor, rate_limits=rate_limits, dump_graphs=dump_generations)
        return self.task_worker.get_result(), stats


class TaskClassProtocol(Protocol[P, R]):
    task_config: ClassVar[TaskConfig]
    task_worker: TaskWorker
    def init(self, *args: P.args, **kwargs: P.kwargs) -> None: ...
    def main(self) -> R: ...


def infer_task_type(cls: Type[TaskClassProtocol[P, R]]) -> Type[Task[P, R]]:
    assert issubclass(cls, Task), f'{cls} must inherit from {Task} to infer task type.'
    return cast(Type[Task[P, R]], cls)


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
            return {'__task__': o.task_worker.to_tuple()}
        else:
            # Let the base class default method raise the TypeError
            return super().default(o)


class Req(Generic[T, R]):
    def __set_name__(self, _: Any, name: str) -> None:
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
    def __get__(self, obj: Task[..., Any], _=None) -> Any:
        x = getattr(obj, self.private_name)
        if isinstance(x, Task):
            return x.task_worker.get_result()
        elif isinstance(x, list):
            return [t.task_worker.get_result() for t in x]
        elif isinstance(x, dict):
            return {k: v.task_worker.get_result() for k, v in x.items()}
        elif isinstance(x, Const):
            return x.value
        else:
            raise TypeError(f'Unsupported requirement type: {type(x)}')

    def get_task_list(self, obj: Task[..., Any]) -> list[Task[..., Any]]:
        x = getattr(obj, self.private_name, None)
        assert x is not None, f'Requirement `{self.public_name}` is not set in {obj}.'
        if isinstance(x, Task):
            return [x]
        elif isinstance(x, list):
            return x
        elif isinstance(x, dict):
            return list(x.values())
        elif isinstance(x, Const):
            return []
        else:
            raise TypeError(f'Unsupported requirement type: {type(x)}')


@dataclass
class Const(Generic[T]):
    value: T

    def get_result(self) -> T:
        return self.value


TaskLike = Task[..., U] | Const[U]


Requires = Req[TaskLike[U], U]
RequiresList = Req[Sequence[TaskLike[U]], list[U]]
RequiresDict = Req[Mapping[K, TaskLike[U]], dict[K, U]]


class DataPath:
    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, obj: Task[..., Any], _=None) -> Path:
        return obj.task_worker.directory / self.name


class DataDir:
    def __get__(self, obj: Task[..., Any], _=None) -> Path:
        return obj.task_worker.directory
