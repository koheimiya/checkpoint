from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Generic, NewType, TypeVar, Any, cast, overload
from typing_extensions import ParamSpec, Concatenate, Self
import os
from pathlib import Path

import logging
import dill
import zlib
import diskcache as dc

from concurrent.futures import ProcessPoolExecutor, Future, wait, FIRST_COMPLETED, Executor
from functools import wraps
import inspect
import json


CHECKPOINT_PATH = Path(os.getenv('CP_CACHE_DIR', './.cache')) / 'checkpoint'
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)


LOGGER = logging.getLogger(__file__)


Json = NewType('Json', str)


K = TypeVar('K')
T = TypeVar('T')
P = ParamSpec('P')
R = TypeVar('R')
D = TypeVar('D')


@dataclass
class Database(Generic[T, D]):
    path: str
    compress_level: int
    result_cache: dc.Cache
    timestamp_cache: dc.Cache

    @classmethod
    def make(cls, path: str, compress_level: int) -> Self:
        return Database(
                path=path,
                compress_level=compress_level,
                result_cache=dc.Cache(path + '/result'),
                timestamp_cache=dc.Cache(path + '/timestamp'),
                )

    def save(self, key: Json, obj: T) -> datetime:
        data = dill.dumps(obj, protocol=dill.HIGHEST_PROTOCOL, byref=True)
        data = zlib.compress(data, level=self.compress_level)
        with self.result_cache as ref:
            ref[key] = data

        timestamp = datetime.now()
        with self.timestamp_cache as ref:
            ref[key] = timestamp.timestamp()
        return timestamp

    def load(self, key: Json) -> T:
        with self.result_cache as ref:
            data = ref[key]
        return dill.loads(zlib.decompress(data))

    def load_timestamp(self, key: Json) -> datetime:
        with self.timestamp_cache as ref:
            ts = ref[key]
        return datetime.fromtimestamp(ts)

    def __contains__(self, key: T) -> bool:
        with self.result_cache as ref:
            return key in ref

    def list_keys(self) -> list[str]:
        with self.result_cache as ref:
            return list(map(str, ref))

    def _get_caches(self) -> list[dc.Cache]:
        return [self.result_cache, self.timestamp_cache]

    def clear(self) -> None:
        for cache in self._get_caches():
            cache.clear()

    def delete(self, key: Json) -> None:
        for cache in self._get_caches():
            with cache as ref:
                del ref[key]


Runner = Callable[[], T]  # Delayed computation
RunnerFactory = Callable[P, Runner[R]]


@dataclass(frozen=True)
class Task(Generic[T]):
    """ Runner with cache """
    runner: Runner[T]
    task_factory: TaskFactory[..., T]
    key: Json
    timestamp: datetime | None

    def set_result(self) -> None:
        db = self.task_factory.db
        out = self.runner()
        db.save(self.key, out)

    def get_result(self) -> T:
        db = self.task_factory.db
        return db.load(self.key)

    def run(self, *, executor: Executor | None = None) -> T:
        return self.run_with_info(executor=executor)[0]

    def run_with_info(self, *, executor: Executor | None = None) -> tuple[T, dict[str, Any]]:
        graph = Graph.build(self)
        if executor is None:
            executor = ProcessPoolExecutor()
        info = run_task_graph(graph=graph, executor=executor)
        return self.get_result(), info

    def clear(self) -> None:
        db = self.task_factory.db
        db.delete(self.key)

    def to_tuple(self) -> tuple[str, Json]:
        return self.task_factory.get_db_path(), self.key


AnyTask = Task[Any]


@dataclass
class TaskFactory(Generic[P, R]):
    runner_factory: RunnerFactory[P, R]
    db: Database
    max_concurrency: int | None

    def get_db_path(self) -> str:
        return self.db.path

    def clear(self) -> None:
        self.db.clear()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Task[R]:
        runner = self.runner_factory(*args, **kwargs)
        key = _serialize_arguments(self.runner_factory, *args, **kwargs)
        try:
            timestamp = self.db.load_timestamp(key)
        except KeyError:
            timestamp = None
        return Task(runner, task_factory=self, key=key, timestamp=timestamp)


@overload
def task(fn: RunnerFactory[P, R]) -> TaskFactory[P, R]: ...
@overload
def task(*, compress_level: int = 0, max_concurrency: int | None = None) -> Callable[[RunnerFactory[P, R]], TaskFactory[P, R]]: ...
def task(*args, **kwargs) -> Any:
    if args:
        fn, = args
        return _task()(fn)
    else:
        return _task(**kwargs)


def _task(
        *, compress_level: int = 0, max_concurrency: int | None = None
        ) -> Callable[[RunnerFactory[P, R]], TaskFactory[P, R]]:
    """ Convert a runner factory into a task factory. """

    def decorator(fn: RunnerFactory[P, R]) -> TaskFactory[P, R]:
        db_path = str(CHECKPOINT_PATH / _serialize_function(fn))
        db = Database.make(path=db_path, compress_level=compress_level)
        return wraps(fn)(
                TaskFactory(runner_factory=fn, db=db, max_concurrency=max_concurrency)
                )
    return decorator


def _serialize_function(fn: Callable[..., Any]) -> str:
    return f'{fn.__module__}.{fn.__qualname__}'


def _normalize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
    params = inspect.signature(fn).bind(*args, **kwargs)
    params.apply_defaults()
    return params.arguments


def _serialize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> Json:
    arguments = _normalize_arguments(fn, *args, **kwargs)
    return cast(Json, json.dumps(arguments))


Connector = Callable[[Callable[Concatenate[T, P], R]], Callable[P, R]]  # Takes (T, *P) -> R and return P -> R

@overload
def requires(task: Task[T]) -> Connector[T, P, R]: ...
@overload
def requires(task: list[Task[T]]) -> Connector[list[T], P, R]: ...
@overload
def requires(task: dict[K, Task[T]]) -> Connector[dict[K, T], P, R]: ...
def requires(task: AnyTask | list[AnyTask] | dict[Any, AnyTask]) -> Any:
    """ Register a task dependency """
    if isinstance(task, Task):
        return requires_single(task)
    elif isinstance(task, list):
        return requires_list(task)
    elif isinstance(task, dict):
        return requires_dict(task)
    else:
        raise TypeError(task)



@dataclass(eq=True, frozen=True)
class Connected(Generic[T, P, R]):
    """ Connect a task to a function. """
    task: Task[T]
    fn: Callable[Concatenate[T, P], R]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        x = self.task.get_result()
        return self.fn(x, *args, **kwargs)

    def get_tasks(self) -> list[AnyTask]:
        return [self.task]


def requires_single(task: Task[T]) -> Connector[T, P, R]:
    """ Register a task dependency """
    def decorator(fn: Callable[Concatenate[T, P], R]) -> Callable[P, R]:
        return Connected(task, fn)
    return decorator


@dataclass(eq=True, frozen=True)
class ListConnected(Generic[T, P, R]):
    """ Connect a list of tasks to a function. """
    tasks: list[Task[T]]
    fn: Callable[Concatenate[list[T], P], R]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        xs = [task.get_result() for task in self.tasks]
        return self.fn(xs, *args, **kwargs)

    def get_tasks(self) -> list[AnyTask]:
        return self.tasks


def requires_list(tasks: list[Task[T]]) -> Connector[list[T], P, R]:
    """ Register a task dependency """
    def decorator(fn: Callable[Concatenate[list[T], P], R]) -> Callable[P, R]:
        return ListConnected(tasks, fn)
    return decorator


@dataclass(eq=True, frozen=True)
class DictConnected(Generic[K, T, P, R]):
    """ Connect a dict of tasks to a function. """
    tasks: dict[K, Task[T]]
    fn: Callable[Concatenate[dict[K, T], P], R]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        xs = {k: task.get_result() for k, task in self.tasks.items()}
        return self.fn(xs, *args, **kwargs)

    def get_tasks(self) -> list[AnyTask]:
        return list(self.tasks.values())


def requires_dict(tasks: dict[K, Task[T]]) -> Connector[dict[K, T], P, R]:
    """ Register a task dependency """
    def decorator(fn: Callable[Concatenate[dict[K, T], P], R]) -> Callable[P, R]:
        return DictConnected(tasks, fn)
    return decorator


def get_upstream(task: AnyTask) -> list[AnyTask]:
    deps: list[Task[Any]] = []
    task_fn = task.runner
    while isinstance(task_fn, (Connected, ListConnected, DictConnected)):
        deps.extend(task_fn.get_tasks())
        task_fn = task_fn.fn
    return deps


@dataclass
class Graph:
    root: Task[Any]
    timestamp: datetime | None  # None indicate update is needed.
    downstream: Task[Any] | None
    upstream_graphs: list[Graph]

    @classmethod
    def build(cls, task: Task[Any], downstream: Task[Any] | None = None) -> Self:
        upstream_graphs = [Graph.build(t, downstream=task) for t in get_upstream(task)]
        timestamp = task.timestamp
        if timestamp is not None:
            upstream_timestamps = [ug.timestamp for ug in upstream_graphs]
            need_update = any(uts is None or timestamp < uts for uts in upstream_timestamps)
            if need_update:
                timestamp = None

        out = Graph(
                root=task,
                timestamp=timestamp,
                downstream=downstream,
                upstream_graphs=upstream_graphs,
                )
        return out


def walk_subgraph_to_update(graph: Graph) -> list[Graph]:
    out: list[Graph] = []
    to_expand: list[Graph] = [graph]
    while to_expand:
        g = to_expand.pop()
        if g.timestamp is None:
            out.append(g)
            to_expand.extend(g.upstream_graphs)
    return out


def _run_task(task_data: bytes) -> tuple[str, Json]:
    task = dill.loads(task_data)
    assert isinstance(task, Task)
    task.set_result()
    return task.to_tuple()


def run_task_graph(graph: Graph, executor: Executor) -> dict[str, Any]:
    """ Consume task graph concurrently.
    """
    active_subgraphs = walk_subgraph_to_update(graph)

    # Parse graph in a flat format
    Key = tuple[str, Json]
    nodes: dict[Key, AnyTask] = {}
    descendants: dict[Key, set[Key]] = {}
    precedents: dict[Key, set[Key]] = {}
    leaves: set[Key] = set()
    nodes_by_path: dict[str, set[Json]] = {}
    for g in active_subgraphs:
        root_key = g.root.to_tuple()
        nodes[root_key] = g.root

        if root_key not in descendants:
            descendants[root_key] = set()
        if g.downstream:
            descendants[root_key].add(g.downstream.to_tuple())

        if g.upstream_graphs:
            if root_key not in precedents:
                precedents[root_key] = set(ug.root.to_tuple() for ug in g.upstream_graphs)
            else:
                assert precedents[root_key] == set(ug.root.to_tuple() for ug in g.upstream_graphs), 'Same tasks have to have the same upstream'
        else:
            leaves.add(root_key)

        path, arg_key = root_key
        if path not in nodes_by_path:
            nodes_by_path[path] = set()
        nodes_by_path[path].add(arg_key)

    stats = {k: len(args) for k, args in nodes_by_path.items()}
    LOGGER.info(f'Following tasks will be called: {stats}')

    # Read concurrency budgets (TODO: optimize)
    budgets: dict[str, int] = {}
    occupied: dict[str, int] = {}
    for g in active_subgraphs:
        task = g.root
        mc = task.task_factory.max_concurrency
        if mc is None:
            mc = 99999
        path = task.task_factory.get_db_path()
        if path in budgets:
            continue
        budgets[path] = mc
        occupied[path] = 0

    # Execute tasks
    with executor as executor:
        in_process: set[Future[Key]] = set()
        while leaves or in_process:
            LOGGER.info(
                    f'desc: {len(descendants)}, prec: {len(precedents)}, leaves: {len(leaves)}, in_process: {len(in_process)}'
                    )

            # Submit all leaf tasks  (TODO: optimize)
            leftover: set[Key] = set()
            for leaf in leaves:
                path = leaf[0]
                if occupied[path] < budgets[path]:
                    occupied[path] += 1
                    future = executor.submit(_run_task, dill.dumps(nodes[leaf], protocol=dill.HIGHEST_PROTOCOL, byref=True))
                    in_process.add(future)
                else:
                    leftover.add(leaf)

            # Wait for the first tasks to complete
            done, in_process = wait(in_process, return_when=FIRST_COMPLETED)

            # Update graph
            leaves = leftover
            for done_future in done:
                done_task = done_future.result()
                path = done_task[0]
                occupied[path] -= 1
                assert occupied[path] >= 0

                nodes.pop(done_task)
                next_tasks = descendants.pop(done_task)
                for next_task in next_tasks:
                    if next_task is None:
                        continue
                    deps = precedents[next_task]
                    deps.remove(done_task)
                    if not deps:
                        precedents.pop(next_task)
                        leaves.add(next_task)

    # Confirm the graph is empty
    assert not descendants, 'Something went wrong'
    assert not precedents, 'Something went wrong'
    assert not leaves, 'Something went wrong'
    assert not in_process, 'Something went wrong'
    assert all(n == 0 for n in occupied.values()), 'Something went wrong'
    return {'stats': stats}
