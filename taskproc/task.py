from __future__ import annotations
import __main__
from collections.abc import Iterable
from contextlib import ContextDecorator, redirect_stderr, redirect_stdout, ExitStack, AbstractContextManager
from dataclasses import asdict, dataclass
from typing import Callable, ClassVar, Concatenate, Generic, Literal, Sequence, Type, TypeVar, Any, cast, final
from typing_extensions import ParamSpec, Protocol, Self
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
import inspect


from .types import JsonStr, TaskKey, JsonDict
from .future import Future, FutureJSONEncoder, FutureMapperMixin
from .database import Database
from .graph import TaskGraph, TaskWorkerProtocol, run_task_graph
from .executors import LocalExecutor


LOGGER = logging.getLogger(__name__)


K = TypeVar('K')
T = TypeVar('T')
S = TypeVar('S')
U = TypeVar('U')
P = ParamSpec('P')
R = TypeVar('R', covariant=True)


class Graph(ContextDecorator, AbstractContextManager):
    _ENABLED: bool = False
    CACHE_DIR: Path = Path.cwd() / '.cache'
    CONFIG_REGISTRY: dict[Type[Task[Any]], TaskConfig[Any]] = {}

    def __init__(self, cache_dir: Path | str):
        self.cache_dir = Path(cache_dir).resolve()

    def __enter__(self):
        cls = type(self)
        assert not cls._ENABLED
        cls._ENABLED = True
        self.orig = cls.CACHE_DIR
        cls.CACHE_DIR = self.cache_dir
        cls.CONFIG_REGISTRY.clear()

    def __exit__(self, __exc_type: type[BaseException] | None, __exc_value: BaseException | None, __traceback: Any) -> bool | None:
        cls = type(self)
        assert cls._ENABLED
        cls._ENABLED = False
        cls.CONFIG_REGISTRY.clear()
        cls.CACHE_DIR = self.orig


class TaskConfig(Generic[R]):
    """ Information specific to a task class of a graph (not instance) """
    def __init__(
            self,
            task_class: Type[Task[R]],
            cache_dir: Path,
            ) -> None:

        self.task_class = task_class
        self.cache_dir = cache_dir
        self.worker_registry: dict[JsonStr, TaskWorker[R]] = {}

    @cached_property
    def db(self) -> Database[R]:
        return Database.make(cache_path=self.cache_dir, name=self.task_class.task_name)

    @cached_property
    def source_timestamp(self) -> datetime:
        source = inspect.getsource(self.task_class)
        formatted_source = ast.unparse(ast.parse(source))
        return self.db.update_source_if_necessary(formatted_source)

    def clear_all(self) -> None:
        self.db.clear()


class TaskWorker(Generic[R]):
    def __init__(self, config: TaskConfig[R], instance: Task[R], arg_key: JsonStr) -> None:
        self.config = config
        self.instance = instance
        self.arg_key = arg_key

        self.cache = config.db.get_instance_dir(
                key=arg_key,
                deps={k: w.cache.path for k, w in self.get_prerequisites().items()},
                )

    @property
    def labels(self) -> tuple[str, ...]:
        _channel = self.instance.task_label
        channels: tuple[str, ...]
        if isinstance(_channel, str):
            channels = (_channel,)
        elif isinstance(_channel, Iterable):
            channels = tuple(_channel)
            assert all(isinstance(q, str) for q in channels)
        else:
            raise ValueError('Invalid channel value:', _channel)
        return (self.instance.task_name,) + channels

    @property
    def source_timestamp(self) -> datetime:
        return self.config.source_timestamp

    @property
    def directory(self) -> Path:
        return self.cache.path

    def to_tuple(self) -> TaskKey:
        return (self.instance.task_name, self.arg_key)

    def get_prerequisites(self) -> dict[str, TaskWorker[Any]]:
        inst = self.instance
        prerequisites: dict[str, TaskWorker] = {}
        for name, f in inst.__dict__.items():
            if isinstance(f, Future):
                for k, worker in f.get_workers().items():
                    assert isinstance(worker, TaskWorker)
                    prerequisites[f'{name}.{k}'] = worker
        return prerequisites

    def peek_timestamp(self) -> datetime | None:
        try:
            # return self.config.db.load_timestamp(self.arg_key)
            return self.cache.get_timestamp()
        except RuntimeError:
            return None

    def dump_error_msg(self) -> str:
        task_info = {
                'name': self.instance.task_name,
                'id': self.cache.task_id,
                }
        msg = ''
        def add_msgline(s: str, prompt = 'LOG > ', end='\n'):
            nonlocal msg
            msg += prompt + s + end

        def peek_file(path: Path, name: str):
            if not path.exists():
                add_msgline(f'(NO {name})')
                return

            add_msgline(f'Here is {name} ({path}):')
            PEEK = 10
            with open(path) as f:
                lines = list(enumerate(f.readlines()))
                n = len(lines)
                digits = len(str(n))
                if n == 0:
                    add_msgline('(EMPTY)')
                elif n <= PEEK * 2:
                    for i, line in lines:
                        prompt = ('LINE {:0'+str(digits)+'d} |').format(i)
                        add_msgline(line, prompt=prompt, end='')
                else:
                    for i, line in lines[:PEEK]:
                        prompt = ('{:0'+str(digits)+'d} |').format(i)
                        add_msgline(line, prompt=prompt, end='')
                    add_msgline('(Too long, skip to the end)')
                    for i, line in lines[-PEEK:]:
                        prompt = ('{:0'+str(digits)+'d} |').format(i)
                        add_msgline(line, prompt=prompt, end='')

        add_msgline(f'Error occurred while running detached task {task_info}', prompt='')
        peek_file(self.cache.stdout_path_caller, name='shell stdout')
        peek_file(self.cache.stderr_path_caller, name='shell stderr')
        peek_file(self.cache.stdout_path, name='detached stdout')
        peek_file(self.cache.stderr_path, name='detached stderr')
        add_msgline(f'For more details, see {str(self.directory)}')
        return msg

    def set_result(self, on_child_process: bool, interactive: bool, prefix_command: str | None = None) -> None:
        if prefix_command is None:
            prefix_command = ''

        self.cache.initialize()

        execute_locally = (on_child_process and prefix_command == '') or interactive
        if execute_locally:
            if prefix_command:
                LOGGER.warning(f'Ignore prefix command and execute locally. {prefix_command=}')
            if interactive:
                res = self.instance.run_task()
            else:
                res = self.run_instance_task_with_captured_output()
            self.cache.save_result(res, compress_level=self.instance.task_compress_level)
        else:
            dir_ref = self.directory / 'tmp'
            if dir_ref.exists():
                shutil.rmtree(dir_ref)
            dir_ref.mkdir()
            try:
                worker_path = Path(dir_ref) / 'worker.pkl'

                with open(worker_path, 'wb') as worker_ref:
                    cloudpickle.dump(self, worker_ref)

                pycmd = f'from taskproc.task import TaskWorker; TaskWorker.run_from_path("{worker_path}")'
                shell_command = ' '.join([prefix_command, sys.executable, '-c', repr(pycmd)])
                with open(self.cache.stdout_path_caller, 'w') as fout:
                    with open(self.cache.stderr_path_caller, 'w') as ferr:
                        res = subprocess.run(
                                shell_command,
                                shell=True, text=True,
                                stdout=fout,
                                stderr=ferr,
                                env=os.environ,
                                )
                res.check_returncode()
            finally:
                shutil.rmtree(dir_ref)

    def run_instance_task_with_captured_output(self) -> R:
        with ExitStack() as stack:
            stdout = stack.enter_context(open(self.cache.stdout_path, 'w+'))
            stderr = stack.enter_context(open(self.cache.stderr_path, 'w+'))
            stack.enter_context(redirect_stdout(stdout))
            stack.callback(lambda: stdout.flush())
            stack.enter_context(redirect_stderr(stderr))
            stack.callback(lambda: stderr.flush())
            return self.instance.run_task()
        raise NotImplementedError('Should not happen')

    @property
    def data_directory(self) -> Path:
        return self.cache.data_dir

    def get_result(self) -> R:
        result_key = '_task__result_'
        res = getattr(self.instance, result_key, None)
        if res is None:
            res = self.cache.load_result()
            setattr(self.instance, result_key, res)
        return res

    def clear(self) -> None:
        self.cache.delete()

    @staticmethod
    def run_from_path(path: Path):
        worker: TaskWorker[Any] = cloudpickle.load(open(path, "rb"))
        res = worker.run_instance_task_with_captured_output()
        worker.cache.save_result(res, worker.instance.task_compress_level)


class PartiallyTypedTask(Protocol[R]):
    def run_task(self) -> R:
        ...


def wrap_task_init(init_method: Callable[Concatenate[Task[R], P], None]) -> Callable[Concatenate[Task[R], P], None]:
    def wrapped_init(self: Task, *args: P.args, **kwargs: P.kwargs) -> None:
        config = self.get_task_config()
        arg_key = _serialize_arguments(self.__init__, *args, **kwargs)
        worker = config.worker_registry.get(arg_key, None)
        # Reuse registered if exists
        if worker is not None:
            self.task_worker = worker
            return

        # Initialize instance
        init_method(self, *args, **kwargs)
        worker = TaskWorker[R](config=config, instance=self, arg_key=arg_key)
        config.worker_registry[arg_key] = worker
        self.task_config = config
        self.task_worker = worker
        return 
    return wrapped_init


class Task(FutureMapperMixin, Generic[R]):
    __init_orig__: Any
    task_config: TaskConfig[R]
    task_worker: TaskWorker[R]
    task_compress_level: int = 9
    task_label: str | Sequence[str] = tuple()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Wrap initializer to make __init__ lazy
        cls.__init_orig__ = cls.__init__
        cls.__init__ = wrap_task_init(cls.__init__)  # type: ignore
        super().__init_subclass__(**kwargs)

    def __init__(self) -> None:
        ...

    def run_task(self) -> R:
        ...

    def get_result(self: PartiallyTypedTask[T]) -> T:
        return cast(Task, self).task_worker.get_result()

    def to_json(self) -> JsonDict:
        name, keys = self.task_worker.to_tuple()
        return JsonDict({'__task__': name, '__args__': json.loads(keys)})

    def get_workers(self) -> dict[str, TaskWorkerProtocol]:
        return {'self': self.task_worker}

    @classmethod
    def get_task_config(cls) -> TaskConfig[R]:
        if not Graph._ENABLED:
            raise RuntimeError(f'{Graph} must be enabled to get a task config.')

        if cls in Graph.CONFIG_REGISTRY:
            return Graph.CONFIG_REGISTRY[cls]

        config = TaskConfig(
                task_class=cls,
                cache_dir=Graph.CACHE_DIR,
                )
        Graph.CONFIG_REGISTRY[cls] = config
        return config

    @classmethod
    @property
    def task_name(cls) -> str:
        return _get_object_full_name(cls)

    @property
    def task_directory(self) -> Path:
        return self.task_worker.data_directory

    def run_graph(
            self: PartiallyTypedTask[T], *,
            executor: Executor | None = None,
            rate_limits: dict[str, int] | None = None,
            detect_source_change: bool = False,
            dump_generations: bool = False,
            show_progress: bool = False,
            prefixes: dict[str, str] | None = None,
            ) -> tuple[T, dict[str, Any]]:
        self = cast(Task, self)
        graph = TaskGraph.build_from(self.task_worker, detect_source_change=detect_source_change)

        if executor is None:
            executor = ProcessPoolExecutor()

        stats = run_task_graph(
                graph=graph,
                executor=executor,
                rate_limits=rate_limits,
                dump_graphs=dump_generations,
                show_progress=show_progress,
                prefixes=prefixes,
                )
        return self.task_worker.get_result(), stats

    @classmethod
    def cli(cls, args: Sequence[str] | None = None, defaults: CLIDefaultArguments | None = None) -> None:
        if defaults is None:
            defaults = CLIDefaultArguments.get_default()
        _run_with_argparse(cls, args=args, defaults=defaults)

    @classmethod
    def clear_all_tasks(cls) -> None:
        cls.get_task_config().clear_all()

    def clear_task(self) -> None:
        self.task_worker.clear()


def _get_object_full_name(fn: Callable[..., Any]) -> str:
    return f'{fn.__module__}.{fn.__qualname__}'


def _normalize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
    params = inspect.signature(fn).bind(*args, **kwargs)
    params.apply_defaults()
    return params.arguments


def _serialize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> JsonStr:
    arguments = _normalize_arguments(fn, *args, **kwargs)
    return cast(JsonStr, json.dumps(arguments, separators=(',', ':'), sort_keys=True, cls=FutureJSONEncoder))


@dataclass(frozen=True)
class CLIDefaultArguments:
    loglevel: Literal['debug', 'info', 'warning', 'error'] = 'warning'
    num_workers: int | None = None
    kwargs: dict[str, Any] | None = None
    prefix: dict[str, Any] | None = None
    rate_limits: dict[str, Any] | None = None
    exec_type: Literal['process', 'thread', 'local'] = 'process'
    _global: ClassVar[Self] | None = None

    def populate(self):
        type(self)._global = self

    @classmethod
    def get_default(cls):
        return CLIDefaultArguments() if cls._global is None else cls._global


def _run_with_argparse(
        task_class: Type[Task[Any]],
        args: Sequence[str] | None,
        defaults: CLIDefaultArguments,
        ) -> None:
    if defaults is None:
        params = argparse.Namespace()
    else:
        params = argparse.Namespace(**asdict(defaults))

    sig = inspect.signature(task_class.__init_orig__)
    param_types = {p.name: p.annotation for _, p in sig.parameters.items() if p.name != 'self'}

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', required=True, type=Path, help='Directory for storing cache.')       
    parser.add_argument('-l', '--loglevel', choices=['debug', 'info', 'warning', 'error'], default=defaults.loglevel, help=f'Defaults to {defaults.loglevel}.')
    parser.add_argument('-n', '--num-workers', type=int, default=None)
    parser.add_argument('--kwargs', type=json.loads, default=defaults.kwargs, help=f'Parameters of entrypoint in JSON dictionary of type: {param_types}. Defaults to {defaults.kwargs}.')
    parser.add_argument('--prefix', type=json.loads, default=defaults.prefix, help=f'Prefix commands per channel in JSON dictionary. Defaults to {defaults.rate_limits}.')
    parser.add_argument('--rate-limits', type=json.loads, default=defaults.rate_limits, help=f'Rate limits per channel in JSON dictionary. Defaults to {defaults.rate_limits}.')
    parser.add_argument('-D', '--disable-detect-source-change', action='store_true', help='Disable automatic source change detection based on AST.')
    parser.add_argument('-t', '--exec-type', choices=['process', 'thread', 'local'], default=defaults.exec_type, help=f'Defaults to {defaults.exec_type}.')
    parser.add_argument('--dont-force-entrypoint', action='store_true', help='Do nothing if the cache of the entripoint task is up-to-date.')       
    parser.add_argument('--dont-show-progress', action='store_true')                                                                                
    parser.parse_args(args=args, namespace=params)

    logging.basicConfig(level=getattr(logging, params.loglevel.upper()))
    LOGGER.info('Parsing args from CLI.')
    LOGGER.info(f'Params: {params}')

    with Graph(cache_dir=params.output):
        task_instance = task_class(**(params.kwargs if params.kwargs is not None else {}))
        if not params.dont_force_entrypoint:
            task_instance.clear_task()
        try:
            _, stats = task_instance.run_graph(
                    executor=_get_executor(params.exec_type, max_workers=params.num_workers),
                    rate_limits=params.rate_limits,
                    detect_source_change=not params.disable_detect_source_change,
                    show_progress=not params.dont_show_progress,
                    prefixes=params.prefix,
                    )
        finally:
            # Fix broken tty after Popen with tricky command. Need some fix in the future.
            os.system('stty sane')

    LOGGER.debug(f"stats:\n{stats}")

    if task_instance.task_worker.cache.stdout_path.exists():
        print("==== ENTRYPOINT STDOUT (DETACHED) ====")
        print(open(task_instance.task_worker.cache.stdout_path).read())
    else:
        print("==== NO ENTRYPOINT STDOUT (DETACHED) ====")

    if task_instance.task_worker.cache.stderr_path.exists():
        print("==== ENTRYPOINT STDERR (DETACHED) ====")
        print(open(task_instance.task_worker.cache.stderr_path).read())
    else:
        print("==== NO ENTRYPOINT STDERR (DETACHED) ====")


def _get_executor(executor_name: Literal['process', 'thread', 'local'] | str, max_workers: int | None) -> Executor:
    if executor_name == 'process':
        executor_type = ProcessPoolExecutor
    elif executor_name == 'thread':
        executor_type = ThreadPoolExecutor
    elif executor_name == 'local':
        executor_type = LocalExecutor
    else:
        raise ValueError('Unrecognized executor name:', executor_name)
    return executor_type(max_workers=max_workers)
