from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import redirect_stderr, redirect_stdout, ExitStack
from typing import Callable, Generic, Mapping, Protocol, Sequence, Type, TypeVar, Any, cast
from typing_extensions import ParamSpec, Self, get_origin, overload
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from concurrent.futures import Executor
from functools import cached_property
import ast
import logging
import inspect
import json
import shutil
import cloudpickle
import gzip
import sys
from .subprocess import run_shell_command


from .types import Json, TaskKey, Context
from .database import Database
from .graph import TaskGraph, run_task_graph


LOGGER = logging.getLogger(__name__)


K = TypeVar('K')
T = TypeVar('T')
S = TypeVar('S')
U = TypeVar('U')
P = ParamSpec('P')
R = TypeVar('R', covariant=True)


class TaskConfig(Generic[P, R]):
    """ Information specific to a task class (not instance) """
    def __init__(
            self,
            task_class: Type[TaskType[P, R]],
            channels: tuple[str, ...],
            compress_level: int,
            prefix_command: str | None,
            detach_output: bool,
            ) -> None:

        self.task_class = task_class
        self.name = _serialize_function(task_class)
        self.compress_level = compress_level
        self.channels = (self.name,) + channels
        self.prefix_command = prefix_command
        self.detach_output = detach_output
        self.worker_registry: dict[Json, TaskWorker[R]] = {}


    @cached_property
    def db(self) -> Database:
        return Database.make(name=self.name, compress_level=self.compress_level)

    @cached_property
    def source_timestamp(self) -> datetime:
        source = inspect.getsource(self.task_class)
        formatted_source = ast.unparse(ast.parse(source))
        return self.db.update_source_if_necessary(formatted_source)

    def clear_all(self) -> None:
        self.db.clear()


class TaskWorker(Generic[R]):
    @classmethod
    def make(cls, config: TaskConfig[P, R], instance: TaskType[P, R], *args: P.args, **kwargs: P.kwargs) -> Self:
        arg_key = _serialize_arguments(instance.build_task, *args, **kwargs)
        worker = config.worker_registry.get(arg_key, None)
        if worker is not None:
            return worker

        instance.build_task(*args, **kwargs)
        worker = TaskWorker[R](config=config, instance=instance, arg_key=arg_key)
        config.worker_registry[arg_key] = worker
        return worker

    def __init__(self, config: TaskConfig[..., R], instance: TaskType[..., R], arg_key: Json) -> None:
        self.config = config
        self.instance = instance
        self.arg_key = arg_key

    @property
    def channels(self) -> tuple[str, ...]:
        return self.config.channels

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
                prerequisites.extend([task._task_worker for task in v.get_task_list(inst)])
        assert all(isinstance(p, TaskWorker) for p in prerequisites)
        return prerequisites

    def peek_timestamp(self) -> datetime | None:
        try:
            return self.config.db.load_timestamp(self.arg_key)
        except KeyError:
            return None

    def set_result(self) -> None:
        db = self.config.db
        if self.data_directory.exists():
            shutil.rmtree(self.data_directory)

        out = self._run_instance_task_with_captured_output()

        db.save(self.arg_key, out)

    def run_instance_task(self) -> R:
        prefix_command = self.config.prefix_command
        if prefix_command is None:
            return self.instance.run_task()
        else:
            dir_ref = self.directory / 'tmp'
            if dir_ref.exists():
                shutil.rmtree(dir_ref)
            dir_ref.mkdir()
            try:
                worker_path = Path(dir_ref) / 'worker.pkl'
                result_path = Path(dir_ref) / 'result.pkl'
                pycmd = f"""import gzip, cloudpickle
                    worker = cloudpickle.load(gzip.open("{worker_path}", "rb"))
                    res = worker.instance.run_task()
                    cloudpickle.dump(res, gzip.open("{result_path}", "wb"))
                """.replace('\n', ';')
                shell_command = ' '.join([prefix_command, sys.executable, '-c', repr(pycmd)])

                with gzip.open(worker_path, 'wb') as worker_ref:
                    cloudpickle.dump(self, worker_ref)

                returncode = run_shell_command(shell_command)
                if returncode != 0:
                    raise RuntimeError(returncode)

                with gzip.open(result_path, 'rb') as result_ref:
                    return cloudpickle.load(result_ref)
            finally:
                shutil.rmtree(dir_ref)

    def _run_instance_task_with_captured_output(self) -> R:
        if not self.config.detach_output:
            return self.run_instance_task()

        try:
            with ExitStack() as stack:
                stdout = stack.enter_context(open(self.stdout_path, 'w+'))
                stderr = stack.enter_context(open(self.stderr_path, 'w+'))
                stack.enter_context(redirect_stdout(stdout))
                stack.enter_context(redirect_stderr(stderr))
                return self.run_instance_task()
        except:
            task_info = {
                    'name': self.config.name,
                    'id': self.task_id,
                    'args': self.task_args,
                    }
            LOGGER.error(f'Error occurred while running detached task {task_info}')
            LOGGER.error(f'Here is the detached stdout ({self.stdout_path}):')
            for line in open(self.stdout_path).readlines():
                LOGGER.error(line)
            LOGGER.error(f'Here is the detached stderr ({self.stderr_path}):')
            for line in open(self.stderr_path).readlines():
                LOGGER.error(line)
            raise
        raise NotImplementedError('Should not happen')

    @property
    def task_id(self) -> int:
        return self.config.db.id_table.get(self.arg_key)

    @property
    def task_args(self) -> dict[str, Any]:
        return json.loads(self.arg_key)

    @property
    def stdout_path(self) -> Path:
        return self.config.db.get_stdout_path(self.arg_key)

    @property
    def stderr_path(self) -> Path:
        return self.config.db.get_stderr_path(self.arg_key)

    @property
    def directory(self) -> Path:
        _, arg_str = self.to_tuple()
        return self.config.db.get_result_dir(arg_str)

    @property
    def _data_directory_uninit(self) -> Path:
        _, arg_str = self.to_tuple()
        return self.config.db.get_data_dir(arg_str)

    @property
    def data_directory(self) -> Path:
        out = self._data_directory_uninit
        out.mkdir(exist_ok=True)
        return out

    def get_result(self) -> R:
        result_key = '_task__result_'
        res = getattr(self.instance, result_key, None)
        if res is None:
            res = self.config.db.load(self.arg_key)
            setattr(self.instance, result_key, res)
        return res

    def clear(self) -> None:
        db = self.config.db
        try:
            db.delete(self.arg_key)
        except KeyError:
            pass
        directory = self._data_directory_uninit
        if directory.exists():
            shutil.rmtree(directory)


class TaskType(Generic[P, R], ABC):
    _task_config: TaskConfig[P, R]

    @abstractmethod
    def build_task(self, *args: P.args, **kwargs: P.kwargs) -> None:
        pass

    @abstractmethod
    def run_task(self) -> R:
        pass

    def __init_subclass__(cls, **kwargs: Any) -> None:
        _channel = kwargs.pop('channel', None)
        channels: tuple[str, ...]
        if isinstance(_channel, str):
            channels = (_channel,)
        elif isinstance(_channel, Iterable):
            channels = tuple(_channel)
            assert all(isinstance(q, str) for q in channels)
        elif _channel is None:
            channels = tuple()
        else:
            raise ValueError('Invalid channel value:', _channel)

        compress_level = kwargs.pop('compress_level', 9)
        prefix_command = kwargs.pop('prefix_command', None)
        detach_output = kwargs.pop('detach_output', True)

        # Fill missing requirement
        ann = inspect.get_annotations(cls, eval_str=True)
        for k, v in ann.items():
            if get_origin(v) is Req and getattr(cls, k, None) is None:
                req = Req()
                req.__set_name__(None, k)
                setattr(cls, k, req)

        cls._task_config = TaskConfig(task_class=cls, channels=channels, compress_level=compress_level, prefix_command=prefix_command, detach_output=detach_output)
        super().__init_subclass__(**kwargs)

    def __init__(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self._task_worker: TaskWorker[R] = TaskWorker.make(
                self._task_config, self, *args, **kwargs
                )

    @classmethod
    @property
    def task_name(cls) -> str:
        return cls._task_config.name

    @property
    def task_directory(self) -> Path:
        return self._task_worker.data_directory

    @property
    def task_id(self) -> int:
        return self._task_worker.task_id

    @property
    def task_args(self) -> dict[str, Any]:
        return self._task_worker.task_args

    @property
    def task_stdout(self) -> Path:
        return self._task_worker.stdout_path

    @property
    def task_stderr(self) -> Path:
        return self._task_worker.stderr_path

    @classmethod
    def clear_all_tasks(cls) -> None:
        cls._task_config.clear_all()

    def clear_task(self) -> None:
        self._task_worker.clear()

    def run_graph(
            self, *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            show_progress: bool = False,
            ) -> R:
        return self.run_graph_with_stats(
                executor=executor,
                max_workers=max_workers,
                rate_limits=rate_limits,
                detect_source_change=detect_source_change,
                show_progress=show_progress,
                )[0]

    def run_graph_with_stats(
            self, *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            dump_generations: bool = False,
            show_progress: bool = False,
            ) -> tuple[R, dict[str, Any]]:
        if detect_source_change is None:
            detect_source_change = Context.detect_source_change
        graph = TaskGraph.build_from(self._task_worker, detect_source_change=detect_source_change)

        if executor is None:
            executor = Context.get_executor(max_workers=max_workers)
        else:
            assert max_workers is None
        stats = run_task_graph(graph=graph, executor=executor, rate_limits=rate_limits, dump_graphs=dump_generations, show_progress=show_progress)
        return self._task_worker.get_result(), stats

    def get_task_result(self) -> R:
        return self._task_worker.get_result()

    def __getitem__(self: TaskType[..., Mapping[K, T]], key: K) -> _MappedTask[K, T]:
        return _MappedTask(self, key)


class TaskClassProtocol(Protocol[P, R]):
    def build_task(self, *args: P.args, **kwargs: P.kwargs) -> None: ...
    def run_task(self) -> R: ...


def infer_task_type(cls: Type[TaskClassProtocol[P, R]]) -> Type[TaskType[P, R]]:
    assert issubclass(cls, TaskType), f'{cls} must inherit from {TaskType} to infer task type.'
    return cast(Type[TaskType[P, R]], cls)


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
        if isinstance(o, TaskType):
            name, keys = o._task_worker.to_tuple()
            return {'__task__': name, '__args__': json.loads(keys)}
        elif isinstance(o, _MappedTask):
            out = self.default(o.get_origin())
            out['__key__'] = o.get_args()
            return out
        else:
            # Let the base class default method raise the TypeError
            return super().default(o)


@dataclass
class _MappedTask(Generic[K, T]):
    task: TaskType[..., Mapping[K, T]] | _MappedTask[Any, Mapping[K, T]]
    key: K

    def get_origin(self) -> TaskType[..., Any]:
        x = self.task
        if isinstance(x, _MappedTask):
            return x.get_origin()
        else:
            return x

    def get_args(self) -> list[Any]:
        out = []
        x = self
        while isinstance(x, _MappedTask):
            out.append(x.key)
            x = x.task
        return out[::-1]

    def get_task_result(self) -> T:
        out = self.get_origin().get_task_result()
        for k in self.get_args():
            out = out[k]
        return out


class Req(Generic[T, R]):
    def __set_name__(self, _: Any, name: str) -> None:
        self.public_name = name
        self.private_name = '_requires__' + name

    def __set__(self, obj: TaskType[..., Any], value: T) -> None:
        setattr(obj, self.private_name, value)

    @overload
    def __get__(self: Requires[U], obj: TaskType[..., Any], _=None) -> U: ...
    @overload
    def __get__(self: RequiresList[U], obj: TaskType[..., Any], _=None) -> list[U]: ...
    @overload
    def __get__(self: RequiresDict[K, U], obj: TaskType[..., Any], _=None) -> dict[K, U]: ...
    def __get__(self, obj: TaskType[..., Any], _=None) -> Any:

        def get_result(task_like: TaskLike[S]) -> S:
            if isinstance(task_like, (TaskType, _MappedTask, Const)):
                return task_like.get_task_result()
            else:
                raise TypeError(f'Unsupported requirement type: {type(task_like)}')

        x = getattr(obj, self.private_name)
        if isinstance(x, list):
            return [get_result(t) for t in x]
        elif isinstance(x, dict):
            return {k: get_result(v) for k, v in x.items()}
        else:
            return get_result(x)

    def get_task_list(self, obj: TaskType[..., Any]) -> list[TaskType[..., Any]]:
        x = getattr(obj, self.private_name, None)
        assert x is not None, f'Requirement `{self.public_name}` is not set in {obj}.'

        if isinstance(x, _MappedTask):
            x = x.get_origin()

        if isinstance(x, TaskType):
            return [x]
        elif isinstance(x, list):
            return x
        elif isinstance(x, dict):
            return list(x.values())
        elif isinstance(x, Const):
            return []
        else:
            raise TypeError(f'Unsupported requirement type: {type(x)}')


@dataclass(frozen=True)
class Const(Generic[R]):
    value: R

    def get_task_result(self) -> R:
        return self.value


Task = TaskType[..., R] | _MappedTask[Any, R]
TaskLike = Task[R] | Const[R]


Requires = Req[TaskLike[R], R]
RequiresList = Req[Sequence[TaskLike[R]], list[R]]
RequiresDict = Req[Mapping[K, TaskLike[R]], dict[K, R]]


class TaskBase(TaskType):
    def build_task(self) -> None:
        pass
    def run_task(self) -> None:
        pass