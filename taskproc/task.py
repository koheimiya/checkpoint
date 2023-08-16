from __future__ import annotations
from collections.abc import Iterable
from contextlib import redirect_stderr, redirect_stdout, ExitStack
from typing import Callable, Generic, Mapping, Protocol, Sequence, Type, TypeVar, Any, cast
from typing_extensions import ParamSpec, Self, get_origin, overload
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from concurrent.futures import Executor
from functools import cached_property, wraps
import ast
import logging
import inspect
import json
import shutil
import cloudpickle
import subprocess
import sys


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


class TaskConfig(Generic[R]):
    """ Information specific to a task class (not instance) """
    def __init__(
            self,
            task_class: Type[TaskBase[R]],
            channels: tuple[str, ...],
            compress_level: int,
            prefix_command: str,
            interactive: bool,
            ) -> None:

        self.task_class = task_class
        self.name = _serialize_function(task_class)
        self.compress_level = compress_level
        self.channels = (self.name,) + channels
        self.prefix_command = prefix_command
        self.interactive = interactive
        if self.interactive and self.prefix_command:
            LOGGER.warning(f'`interactive` is set True, nullifying the effect of `prefix_command`={self.prefix_command!r}')
        self.worker_registry: dict[Json, TaskWorker[R]] = {}


    @cached_property
    def db(self) -> Database[R]:
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
    def make(cls, task_config: TaskConfig[R], task_instance: TaskBase[R], *args: Any, **kwargs: Any) -> Self:
        arg_key = _serialize_arguments(task_instance._build_task, *args, **kwargs)
        worker = task_config.worker_registry.get(arg_key, None)
        if worker is not None:
            return worker

        task_instance._build_task(*args, **kwargs)
        worker = TaskWorker[R](config=task_config, instance=task_instance, arg_key=arg_key)
        task_config.worker_registry[arg_key] = worker
        return worker

    def __init__(self, config: TaskConfig[R], instance: TaskBase[R], arg_key: Json) -> None:
        self.config = config
        self.instance = instance
        self.arg_key = arg_key
        self.dirobj = config.db.get_instance_dir(
                key=arg_key,
                deps={k: w.dirobj.path for k, w in self.get_prerequisites().items()}
                )

    @property
    def channels(self) -> tuple[str, ...]:
        return self.config.channels

    @property
    def source_timestamp(self) -> datetime:
        return self.config.source_timestamp

    def to_tuple(self) -> TaskKey:
        return (self.config.name, self.arg_key)

    def get_prerequisites(self) -> dict[str, TaskWorker[Any]]:
        cls = self.config.task_class
        inst = self.instance
        prerequisites: dict[str, TaskWorker[Any]] = {}
        for name, v in inspect.getmembers(cls):
            if isinstance(v, Req):
                for k, task in v.get_task_dict(inst).items():
                    if k is None:
                        prerequisites[f'{name}'] = task._task_worker
                    else:
                        prerequisites[f'{name}.{k}'] = task._task_worker
        assert all(isinstance(p, TaskWorker) for p in prerequisites.values())
        return prerequisites

    def peek_timestamp(self) -> datetime | None:
        try:
            # return self.config.db.load_timestamp(self.arg_key)
            return self.dirobj.get_timestamp()
        except RuntimeError:
            return None

    def set_result(self, execute_locally: bool = False, force_interactive: bool  = False) -> None:
        self.dirobj.initialize()
        self.run_and_save_instance_task(execute_locally=execute_locally, force_interactive=force_interactive)

    def log_error(self) -> None:
        if not self.config.interactive:
            task_info = {
                    'name': self.config.name,
                    'id': self.task_id,
                    'args': self.task_args,
                    }
            LOGGER.error(f'Error occurred while running detached task {task_info}')
            LOGGER.error(f'Here is the detached stdout ({self.stdout_path}):')
            with open(self.stdout_path) as f:
                LOGGER.error(f.read())
            LOGGER.error(f'Here is the detached stderr ({self.stderr_path}):')
            with open(self.stderr_path) as f:
                LOGGER.error(f.read())

    def run_and_save_instance_task(self, execute_locally: bool, force_interactive: bool) -> None:
        if self.config.interactive or force_interactive:
            if self.config.prefix_command:
                LOGGER.warning(f'Ignore prefix command and enter interactive mode. {self.config.prefix_command=}')
            res = self.instance.run_task()
            # self.config.db.save(self.arg_key, res)
            self.dirobj.save_result(res)
        elif execute_locally and self.config.prefix_command == '':
            res = self.run_instance_task_with_captured_output()
            # self.config.db.save(self.arg_key, res)
            self.dirobj.save_result(res)
        else:
            dir_ref = self.directory / 'tmp'
            if dir_ref.exists():
                shutil.rmtree(dir_ref)
            dir_ref.mkdir()
            try:
                worker_path = Path(dir_ref) / 'worker.pkl'
                pycmd = f"""import pickle
worker = pickle.load(open("{worker_path}", "rb"))
res = worker.run_instance_task_with_captured_output()
worker.dirobj.save_result(res)
""".replace('\n', '; ')

                with open(worker_path, 'wb') as worker_ref:
                    cloudpickle.dump(self, worker_ref)

                shell_command = ' '.join([self.config.prefix_command, sys.executable, '-c', repr(pycmd)])
                res = subprocess.run(
                        shell_command,
                        shell=True, text=True,
                        capture_output=True,
                        )
                def _prepend(path: Path, text: str):
                    try:
                        original_contents = open(path, 'r').read()
                    except:
                        original_contents = f'<error while loading {str(path)}>'

                    with open(path, 'w') as f:
                        f.write('=== caller log ===\n')
                        f.write(text)
                        f.write('=== callee log ===\n')
                        f.write(original_contents)
                _prepend(self.stdout_path, res.stdout)
                _prepend(self.stderr_path, res.stderr)
                res.check_returncode()
            finally:
                shutil.rmtree(dir_ref)

    def run_instance_task_with_captured_output(self) -> R:
        with ExitStack() as stack:
            stdout = stack.enter_context(open(self.stdout_path, 'w+'))
            stderr = stack.enter_context(open(self.stderr_path, 'w+'))
            stack.enter_context(redirect_stdout(stdout))
            stack.callback(lambda: stdout.flush())
            stack.enter_context(redirect_stderr(stderr))
            stack.callback(lambda: stderr.flush())
            return self.instance.run_task()
        raise NotImplementedError('Should not happen')

    @property
    def task_id(self) -> int:
        return self.dirobj.task_id

    @property
    def task_args(self) -> dict[str, Any]:
        return json.loads(self.arg_key)

    @property
    def is_interactive(self) -> bool:
        return self.config.interactive

    @property
    def stdout_path(self) -> Path:
        return self.dirobj.stdout_path

    @property
    def stderr_path(self) -> Path:
        return self.dirobj.stderr_path

    @property
    def directory(self) -> Path:
        return self.dirobj.path

    @property
    def data_directory(self) -> Path:
        return self.dirobj.data_dir

    def get_result(self) -> R:
        result_key = '_task__result_'
        res = getattr(self.instance, result_key, None)
        if res is None:
            res = self.dirobj.load_result()
            setattr(self.instance, result_key, res)
        return res

    def clear(self) -> None:
        self.dirobj.delete()


class TaskBase(Generic[R]):
    _task_config: TaskConfig[R]

    def __init__(self) -> None:
        ...

    def __task_init__(self, *args: Any, **kwargs: Any) -> None:
        self._task_worker: TaskWorker[R] = TaskWorker.make(
                self._task_config, self, *args, **kwargs
                )

    def _build_task(self, *args: Any, **kwargs: Any) -> None:
        ...

    def run_task(self) -> R:
        ...

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
        prefix_command = kwargs.pop('prefix_command', '')
        interactive = kwargs.pop('interactive', False)

        # Fill missing requirement
        ann = inspect.get_annotations(cls, eval_str=True)
        for k, v in ann.items():
            if get_origin(v) is Req and getattr(cls, k, None) is None:
                req = Req()
                req.__set_name__(None, k)
                setattr(cls, k, req)

        cls._task_config = TaskConfig(
                task_class=cls,
                channels=channels,
                compress_level=compress_level,
                prefix_command=prefix_command,
                interactive=interactive,
                )

        # Swap initializer to make __init__ lazy
        cls._build_task = cls.__init__  # type: ignore
        cls.__init__ = wraps(cls._build_task)(cls.__task_init__)
        super().__init_subclass__(**kwargs)

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
            self: TaskClassProtocol[T], *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            show_progress: bool = False,
            force_interactive: bool = False,
            ) -> T:
        assert isinstance(self, TaskBase)
        return self.run_graph_with_stats(
                executor=executor,
                max_workers=max_workers,
                rate_limits=rate_limits,
                detect_source_change=detect_source_change,
                show_progress=show_progress,
                force_interactive=force_interactive,
                )[0]

    def run_graph_with_stats(
            self: TaskClassProtocol[T], *,
            executor: Executor | None = None,
            max_workers: int | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool | None = None,
            dump_generations: bool = False,
            show_progress: bool = False,
            force_interactive: bool = False,
            ) -> tuple[T, dict[str, Any]]:
        assert isinstance(self, TaskBase)
        if detect_source_change is None:
            detect_source_change = Context.detect_source_change
        graph = TaskGraph.build_from(self._task_worker, detect_source_change=detect_source_change)

        if executor is None:
            executor = Context.get_executor(max_workers=max_workers)
        else:
            assert max_workers is None

        stats = run_task_graph(
                graph=graph,
                executor=executor,
                rate_limits=rate_limits,
                dump_graphs=dump_generations,
                show_progress=show_progress,
                force_interactive=force_interactive,
                )
        return self._task_worker.get_result(), stats

    def get_task_result(self) -> R:
        return self._task_worker.get_result()

    @overload
    def __getitem__(self: TaskClassProtocol[Sequence[T]], key: int) -> _MappedTask[T]: ...
    @overload
    def __getitem__(self: TaskClassProtocol[Mapping[K, T]], key: K) -> _MappedTask[T]: ...
    def __getitem__(self: TaskClassProtocol[Mapping[K, T] | Sequence[T]], key: int | K) -> _MappedTask[T]:
        return _MappedTask(self, key)


class TaskClassProtocol(Protocol[R]):
    def __task_init__(self, *args: Any, **kwargs: Any) -> None: ...
    def run_task(self) -> R: ...


def cast_task(task: TaskClassProtocol[R]) -> TaskBase[R]:
    return cast(TaskBase[R], task)


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
        if isinstance(o, TaskBase):
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
class _MappedTask(Generic[R]):
    task: TaskClassProtocol[Mapping[Any, R] | Sequence[R]] | _MappedTask[Mapping[Any, R] | Sequence[R]]
    key: Any

    def get_origin(self) -> TaskBase[Any]:
        x = self.task
        if isinstance(x, _MappedTask):
            return x.get_origin()
        else:
            return cast_task(x)

    def get_args(self) -> list[Any]:
        out = []
        x = self
        while isinstance(x, _MappedTask):
            out.append(x.key)
            x = x.task
        return out[::-1]

    def get_task_result(self) -> R:
        out = self.get_origin().get_task_result()
        for k in self.get_args():
            out = out[k]
        return out

    @overload
    def __getitem__(self: _MappedTask[Sequence[T]], key: int) -> _MappedTask[T]: ...
    @overload
    def __getitem__(self: _MappedTask[Mapping[K, T]], key: K) -> _MappedTask[T]: ...
    def __getitem__(self: _MappedTask[Mapping[K, T] | Sequence[T]], key: int | K) -> _MappedTask[T]:
        return _MappedTask(self, key)


class Req(Generic[T, R]):
    def __set_name__(self, _: Any, name: str) -> None:
        self.public_name = name
        self.private_name = '_requires__' + name

    def __set__(self, obj: TaskBase[Any], value: T) -> None:
        setattr(obj, self.private_name, value)

    @overload
    def __get__(self: Requires[U], obj: TaskBase[Any], _=None) -> U: ...
    @overload
    def __get__(self: RequiresList[U], obj: TaskBase[Any], _=None) -> list[U]: ...
    @overload
    def __get__(self: RequiresDict[K, U], obj: TaskBase[Any], _=None) -> dict[K, U]: ...
    def __get__(self, obj: TaskBase[Any], _=None) -> Any:

        def get_result(task_like: Task[S]) -> S:
            if isinstance(task_like, (TaskBase, _MappedTask, Const)):
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

    def get_task_dict(self, obj: TaskBase[Any]) -> dict[str | None, TaskBase[Any]]:
        x = getattr(obj, self.private_name, None)
        assert x is not None, f'Requirement `{self.public_name}` is not set in {obj}.'

        if isinstance(x, _MappedTask):
            x = x.get_origin()

        if isinstance(x, TaskBase):
            return {None: x}
        elif isinstance(x, list):
            return {str(i): xi for i, xi in enumerate(x)}
        elif isinstance(x, dict):
            return {str(k): v for k, v in x.items()}
        elif isinstance(x, Const):
            return {}
        else:
            raise TypeError(f'Unsupported requirement type: {type(x)}')


@dataclass(frozen=True)
class Const(Generic[R]):
    value: R

    def get_task_result(self) -> R:
        return self.value


RealTask = TaskClassProtocol[R] | _MappedTask[R]
Task = RealTask[R] | Const[R]


Requires = Req[Task[R], R]
RequiresList = Req[Sequence[Task[R]], list[R]]
RequiresDict = Req[Mapping[K, Task[R]], dict[K, R]]
