from __future__ import annotations
from collections.abc import Iterable
from contextlib import ContextDecorator, redirect_stderr, redirect_stdout, ExitStack, AbstractContextManager
from typing import Callable, Concatenate, Generic, Literal, Mapping, Sequence, Type, TypeVar, Any, cast
from typing_extensions import ParamSpec, get_origin, overload
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from functools import cached_property
import argparse
import os
import ast
import logging
import inspect
import json
import shutil
import cloudpickle
import subprocess
import sys


from .types import Json, TaskKey
from .database import Database
from .graph import TaskGraph, run_task_graph, FailedTaskError


LOGGER = logging.getLogger(__name__)


K = TypeVar('K')
T = TypeVar('T')
S = TypeVar('S')
U = TypeVar('U')
P = ParamSpec('P')
R = TypeVar('R', covariant=True)


class Cache(ContextDecorator, AbstractContextManager):
    _ENABLED: bool = False
    CACHE_DIR: Path = Path.cwd() / '.cache'
    TASK_CONFIG_HISTORY: list[TaskConfig[Any]] = []

    def __init__(self, cache_dir: Path | str):
        self.cache_dir = Path(cache_dir).resolve()

    def __enter__(self):
        cls = type(self)
        assert not cls._ENABLED
        cls._ENABLED = True
        self.orig = cls.CACHE_DIR
        cls.CACHE_DIR = self.cache_dir
        cls.TASK_CONFIG_HISTORY.clear()

    def __exit__(self, __exc_type: type[BaseException] | None, __exc_value: BaseException | None, __traceback: Any) -> bool | None:
        cls = type(self)
        assert cls._ENABLED
        cls._ENABLED = False
        cls.TASK_CONFIG_HISTORY.clear()
        cls.CACHE_DIR = self.orig



class TaskConfig(Generic[R]):
    """ Information specific to a task class (not instance) """
    def __init__(
            self,
            task_class: Type[TaskBase[R]],
            cache_dir: Path,
            channels: tuple[str, ...],
            compress_level: int,
            prefix_command: str,
            ) -> None:

        self.task_class = task_class
        self.cache_dir = cache_dir
        self.name = _serialize_function(task_class)
        self.compress_level = compress_level
        self.channels = (self.name,) + channels
        self.prefix_command = prefix_command
        self.worker_registry: dict[Json, TaskWorker[R]] = {}

    @cached_property
    def db(self) -> Database[R]:
        return Database.make(cache_path=self.cache_dir, name=self.name, compress_level=self.compress_level)

    @cached_property
    def source_timestamp(self) -> datetime:
        source = inspect.getsource(self.task_class)
        formatted_source = ast.unparse(ast.parse(source))
        return self.db.update_source_if_necessary(formatted_source)

    def clear_all(self) -> None:
        self.db.clear()


class TaskWorker(Generic[R]):
    def __init__(self, config: TaskConfig[R], instance: TaskBase[R], arg_key: Json) -> None:
        self.config = config
        self.instance = instance
        self.arg_key = arg_key

        self.check_requirements_set()
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

    def check_requirements_set(self) -> None:
        cls = self.config.task_class
        inst = self.instance
        unset = []
        for name, v in inspect.getmembers(cls):
            if isinstance(v, Req):
                if not v.is_set_on(inst):
                    unset.append(name)
        if unset:
            raise ValueError(f'Some requirements are not set on the task {cls.__name__}: {unset}')

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

    def log_error(self) -> None:
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

    def set_result(self, execute_locally: bool, force_interactive: bool, prefix_command: str | None = None) -> None:
        self.dirobj.initialize()
        prefix = prefix_command if prefix_command is not None else self.config.prefix_command
        if force_interactive:
            if prefix:
                LOGGER.warning(f'Ignore prefix command and enter interactive mode. {prefix=}')
            res = self.instance.run_task()
            # self.config.db.save(self.arg_key, res)
            self.dirobj.save_result(res)
        elif execute_locally and prefix == '':
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

                shell_command = ' '.join([prefix, sys.executable, '-c', repr(pycmd)])
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


def wrap_task_init(init_method: Callable[Concatenate[TaskBase[R], P], None]) -> Callable[Concatenate[TaskBase[R], P], None]:
    def wrapped_init(self: TaskBase, *args: P.args, **kwargs: P.kwargs) -> None:
        config = self.task_config
        arg_key = _serialize_arguments(self.__init__, *args, **kwargs)
        worker = config.worker_registry.get(arg_key, None)
        # Reuse registered if exists
        if worker is not None:
            self._task_worker = worker
            return

        # Initialize instance
        init_method(self, *args, **kwargs)
        worker = TaskWorker[R](config=config, instance=self, arg_key=arg_key)
        config.worker_registry[arg_key] = worker
        self._task_worker = worker
        return 
    return wrapped_init


class TaskBase(Generic[R]):
    _task_config: TaskConfig[R]
    _task_worker: TaskWorker[R]
    _task_compress_level: int = 9
    _task_prefix_command: str = ''
    _task_channel: str | Sequence[str] | None = None

    def __init__(self) -> None:
        ...

    def run_task(self) -> R:
        ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Fill missing requirement
        ann = inspect.get_annotations(cls, eval_str=True)
        for k, v in ann.items():
            if get_origin(v) is Req and getattr(cls, k, None) is None:
                req = Req()
                req.__set_name__(None, k)
                setattr(cls, k, req)

        # Wrap initializer to make __init__ lazy
        cls.__init__ = wrap_task_init(cls.__init__)  # type: ignore
        super().__init_subclass__(**kwargs)

    @classmethod
    @property
    def task_config(cls) -> TaskConfig[R]:
        if not Cache._ENABLED:
            raise RuntimeError(f'{Cache} must be enabled to access `task_config`')

        config = getattr(cls, '_task_config', None)
        if config is not None and config in Cache.TASK_CONFIG_HISTORY:
            return config

        _channel = cls._task_channel
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
        config = TaskConfig(
                task_class=cls,
                cache_dir=Cache.CACHE_DIR,
                channels=channels,
                compress_level=cls._task_compress_level,
                prefix_command=cls._task_prefix_command,
                )
        Cache.TASK_CONFIG_HISTORY.append(config)
        cls._task_config = config
        return config

    @classmethod
    @property
    def task_name(cls) -> str:
        return cls.task_config.name

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
        cls.task_config.clear_all()

    def clear_task(self) -> None:
        self._task_worker.clear()

    def run_graph(
            self, *,
            executor: Executor | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool = False,
            dump_generations: bool = False,
            show_progress: bool = False,
            force_interactive: bool = False,
            prefixes: dict[str, str] | None = None,
            ) -> tuple[R, dict[str, Any]]:
        assert isinstance(self, TaskBase)
        graph = TaskGraph.build_from(self._task_worker, detect_source_change=detect_source_change)

        if executor is None:
            executor = ProcessPoolExecutor()

        stats = run_task_graph(
                graph=graph,
                executor=executor,
                rate_limits=rate_limits,
                dump_graphs=dump_generations,
                show_progress=show_progress,
                force_interactive=force_interactive,
                prefixes=prefixes,
                )
        return self._task_worker.get_result(), stats

    @classmethod
    def parse_cli_args(cls, args: Sequence[str] | None = None, defaults: argparse.Namespace | None = None) -> None:
        _run_with_argparse(cls, args=args, defaults=defaults)

    def get_task_result(self) -> R:
        return self._task_worker.get_result()

    @overload
    def __getitem__(self: TaskBase[Sequence[T]], key: int) -> _MappedTask[T]: ...
    @overload
    def __getitem__(self: TaskBase[Mapping[K, T]], key: K) -> _MappedTask[T]: ...
    def __getitem__(self: TaskBase[Mapping[K, T] | Sequence[T]], key: int | K) -> _MappedTask[T]:
        return _MappedTask(self, key)


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
    task: TaskBase[Mapping[Any, R] | Sequence[R]] | _MappedTask[Mapping[Any, R] | Sequence[R]]
    key: Any

    def get_origin(self) -> TaskBase[Any]:
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
    def is_set_on(self, obj: TaskBase[Any]) -> bool:
        return hasattr(obj, self.private_name)

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


RealTask = TaskBase[R] | _MappedTask[R]
Task = RealTask[R] | Const[R]


Requires = Req[Task[R], R]
RequiresList = Req[Sequence[Task[R]], list[R]]
RequiresDict = Req[Mapping[K, Task[R]], dict[K, R]]


def _run_with_argparse(
        task_class: Type[TaskBase[Any]],
        args: Sequence[str] | None,
        defaults: argparse.Namespace | None,
        ) -> None:
    if defaults is None:
        params = argparse.Namespace()
    else:
        params = defaults
    parser = argparse.ArgumentParser()
    parser.add_argument('result_dir', type=Path, help='Path to result directory.')       
    parser.add_argument('-e', '--entrypoint', default='Main', help='Task name for entrypoint.')                                              
    parser.add_argument('-t', '--exec-type', choices=['process', 'thread'], default='process')                                         
    parser.add_argument('-w', '--max-workers', type=int, default=None)                                                                       
    parser.add_argument('-i', '--interactive', action='store_true')                                                                                 
    parser.add_argument('-D', '--disable-detect-source-change', action='store_true', help='Disable automatic source change detection based on AST.')
    parser.add_argument('-l', '--loglevel', choices=['debug', 'info', 'warning', 'error'], default='warning')                     
    parser.add_argument('--kwargs', type=json.loads, default=None, help='Parameters of entrypoint.')                                         
    parser.add_argument('--rate-limits', type=json.loads, default=None, help='JSON dictionary for rate_limits.')                             
    parser.add_argument('--dont-force-entrypoint', action='store_true', help='Do nothing if the cache of the entripoint task is up-to-date.')       
    parser.add_argument('--dont-show-progress', action='store_true')                                                                                
    parser.add_argument('--prefix', type=json.loads, default=None, help='Prefix commands per channels.')                                                                                
    parser.parse_args(args=args, namespace=params)

    logging.basicConfig(level=getattr(logging, params.loglevel.upper()))
    LOGGER.info('Parsing args from CLI.')
    LOGGER.info(f'Params: {params}')

    with Cache(cache_dir=params.result_dir):
        task_instance = task_class(**(params.kwargs if params.kwargs is not None else {}))
        if not params.dont_force_entrypoint:
            task_instance.clear_task()
        try:
            _, stats = task_instance.run_graph(
                    executor=_get_executor(params.exec_type, max_workers=params.max_workers),
                    rate_limits=params.rate_limits,
                    detect_source_change=not params.disable_detect_source_change,
                    show_progress=not params.dont_show_progress,
                    force_interactive=params.interactive,
                    prefixes=params.prefix,
                    )
        except FailedTaskError as e:
            os.system('stty sane')  # Fix broken tty after Popen with tricky command. Need some fix in the future.
            e.task.log_error()
            raise
        else:
            os.system('stty sane')

    LOGGER.debug(f"stats:\n{stats}")

    if task_instance.task_stdout.exists():
        print("==== ENTRYPOINT STDOUT (DETACHED) ====")
        print(open(task_instance.task_stdout).read())
    else:
        print("==== NO ENTRYPOINT STDOUT (DETACHED) ====")

    if task_instance.task_stderr.exists():
        print("==== ENTRYPOINT STDERR (DETACHED) ====")
        print(open(task_instance.task_stderr).read())
    else:
        print("==== NO ENTRYPOINT STDERR (DETACHED) ====")


def _get_executor(executor_name: Literal['process', 'thread'] | str, max_workers: int | None) -> Executor:
    if executor_name == 'process':
        executor_type = ProcessPoolExecutor
    elif executor_name == 'thread':
        executor_type = ThreadPoolExecutor
    else:
        raise ValueError('Unrecognized executor name:', executor_name)
    return executor_type(max_workers=max_workers)
