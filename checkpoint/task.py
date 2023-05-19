from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from typing import Callable, Generic, Mapping, Protocol, Sequence, Type, TypeVar, Any, cast
from typing_extensions import ParamSpec, Self, get_origin, overload
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from concurrent.futures import Executor
import ast
import logging
import inspect
import json
import shutil
import subprocess
import tempfile
import cloudpickle
import gzip
import sys
import io


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
            job_prefix: list[str] | None,
            capture_output: bool,
            ) -> None:

        self.task_class = task_class
        self.name = _serialize_function(task_class)
        self.db = Database.make(name=self.name, compress_level=compress_level)
        self.channels = (self.name,) + channels
        self.job_prefix = job_prefix
        self.capture_output = capture_output
        self.worker_registry: dict[Json, TaskWorker[R]] = {}

        source = inspect.getsource(task_class)
        formatted_source = ast.unparse(ast.parse(source))
        self.source_timestamp = self.db.update_source_if_necessary(formatted_source)

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
        if self.directory.exists():
            shutil.rmtree(self.directory)
        out = self.run_instance_task()
        db.save(self.arg_key, out)

    def run_instance_task(self) -> R:
        job_prefix = self.config.job_prefix
        if job_prefix is None:
            return self._run_task_with_captured_output()
        else:
            with tempfile.TemporaryDirectory() as dir_ref:
                worker_path = Path(dir_ref) / 'worker.pkl'
                result_path = Path(dir_ref) / 'result.pkl'
                pycmd = f"""import gzip, cloudpickle
                    worker = cloudpickle.load(gzip.open("{worker_path}", "rb"))
                    res = worker._run_task_with_captured_output()
                    cloudpickle.dump(res, gzip.open("{result_path}", "wb"))
                """.replace('\n', ';')
                with gzip.open(worker_path, 'wb') as worker_ref:
                    cloudpickle.dump(self, worker_ref)
                res = subprocess.run(
                        [*job_prefix, sys.executable, '-c', pycmd],
                        capture_output=True,
                        )
                print(res.stdout.decode(), end='')
                print(res.stderr.decode(), end='', file=sys.stderr)
                res.check_returncode()
                with gzip.open(result_path, 'rb') as result_ref:
                    return cloudpickle.load(result_ref)

    def _run_task_with_captured_output(self) -> R:
        if not self.config.capture_output:
            return self.instance.run_task()

        stdout = io.TextIOWrapper(io.BytesIO())
        stderr = io.TextIOWrapper(io.BytesIO())
        out, err = b'', b''
        try:
            try:
                with redirect_stdout(stdout):
                    with redirect_stderr(stderr):
                        return self.instance.run_task()
            finally:
                stdout.seek(0)
                out = stdout.read()
                stderr.seek(0)
                err = stderr.read()
                with open(self.stdout_path, 'w') as ref:
                    ref.write(out)
                with open(self.stderr_path, 'w') as ref:
                    ref.write(err)
        except:
            task_info = {
                    'name': self.config.name,
                    'id': self.task_id,
                    'args': self.task_args,
                    }
            LOGGER.error(f'Error occurred while running task {task_info}')
            LOGGER.error(f'Here is the stdout ({self.stdout_path}):')
            for line in out.splitlines():
                LOGGER.error(line)
            LOGGER.error(f'Here is the stderr ({self.stderr_path}):')
            for line in err.splitlines():
                LOGGER.error(line)
            raise

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
    def _directory_uninit(self) -> Path:
        _, arg_str = self.to_tuple()
        return self.config.db.get_data_dir(arg_str)

    @property
    def directory(self) -> Path:
        out = self._directory_uninit
        out.mkdir(exist_ok=True)
        return out

    def get_result(self) -> R:
        return self.config.db.load(self.arg_key)

    def clear(self) -> None:
        db = self.config.db
        try:
            db.delete(self.arg_key)
        except KeyError:
            pass
        directory = self._directory_uninit
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
        job_prefix = kwargs.pop('job_prefix', None)
        capture_output = kwargs.pop('capture_output', True)

        # Fill missing requirement
        ann = inspect.get_annotations(cls, eval_str=True)
        for k, v in ann.items():
            if get_origin(v) is Req and getattr(cls, k, None) is None:
                req = Req()
                req.__set_name__(None, k)
                setattr(cls, k, req)

        cls._task_config = TaskConfig(task_class=cls, channels=channels, compress_level=compress_level, job_prefix=job_prefix, capture_output=capture_output)
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
        return self._task_worker.directory

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
