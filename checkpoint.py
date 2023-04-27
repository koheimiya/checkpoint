from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Generic, NewType, Type, TypeVar, Any, cast, overload
from typing_extensions import ParamSpec, Concatenate, Self
import os
from pathlib import Path

import logging
import cloudpickle
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
R = TypeVar('R', covariant=True)
D = TypeVar('D')


Serializer = tuple[Callable[[Any], bytes], Callable[[bytes], Any]]
DEFAULT_SERIALIZER: Serializer = (cloudpickle.dumps, cloudpickle.loads)


@dataclass(frozen=True)
class Database(Generic[T, D]):
    path: str
    compress_level: int
    result_cache: dc.Cache
    timestamp_cache: dc.Cache
    serializer: Serializer = DEFAULT_SERIALIZER

    @classmethod
    def make(cls, path: str, compress_level: int) -> Self:
        return Database(
                path=path,
                compress_level=compress_level,
                result_cache=dc.Cache(path + '/result'),
                timestamp_cache=dc.Cache(path + '/timestamp'),
                )

    def _dumps(self, obj: Any) -> bytes:
        dumps, _ = self.serializer
        return zlib.compress(dumps(obj), level=self.compress_level)

    def _loads(self, data: bytes) -> Any:
        _, loads = self.serializer
        return loads(zlib.decompress(data))

    def save(self, key: Json, obj: T) -> datetime:
        with self.result_cache as ref:
            ref[key] = self._dumps(obj)

        timestamp = datetime.now()
        with self.timestamp_cache as ref:
            ref[key] = timestamp.timestamp()
        return timestamp

    def load(self, key: Json) -> T:
        with self.result_cache as ref:
            data = ref[key]
        return self._loads(data)

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


Runner = Callable[[], R]  # Delayed computation
RunnerFactory = Callable[P, Runner[R]]


@dataclass(frozen=True)
class Task(Generic[R]):
    """ Runner with cache """
    runner: Runner[R]
    task_factory: TaskFactory[..., R]
    key: Json
    timestamp: datetime | None

    def set_result(self) -> None:
        db = self.task_factory.db
        out = self.runner()
        db.save(self.key, out)

    def get_result(self) -> R:
        db = self.task_factory.db
        return db.load(self.key)

    def run(self, *, executor: Executor | None = None) -> R:
        return self.run_with_info(executor=executor)[0]

    def run_with_info(self, *, executor: Executor | None = None) -> tuple[R, dict[str, Any]]:
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


AnyTask = Task[Any]
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


def run_task_graph(graph: Graph, executor: Executor) -> dict[str, Any]:
    """ Consume task graph concurrently.
    """
    active_subgraphs = walk_subgraph_to_update(graph)

    # Parse graph in a flat format
    Key = tuple[str, Json]
    nodes: dict[Key, AnyTask] = {g.root.to_tuple(): g.root for g in active_subgraphs}
    task_factories: dict[str, TaskFactory[..., Any]] = {k[0]: nodes[k].task_factory for k in nodes}

    descendants: dict[Key, set[Key]] = {}
    precedents: dict[Key, set[Key]] = {}
    node_groups: dict[str, set[Json]] = {}
    for g in active_subgraphs:
        root_key = g.root.to_tuple()
        assert nodes[root_key].to_tuple() == root_key
        assert task_factories[g.root.task_factory.get_db_path()] is g.root.task_factory

        # Aggregate downsteram
        downstream_keys = set() if g.downstream is None else {g.downstream.to_tuple()}.intersection(nodes)
        if root_key not in descendants:
            descendants[root_key] = downstream_keys
        else:
            descendants[root_key].update(downstream_keys)

        # Upstream should be the same if the root is the same
        upstream_keys = set(ug.root.to_tuple() for ug in g.upstream_graphs).intersection(nodes)
        if root_key not in precedents:
            precedents[root_key] = upstream_keys
        else:
            assert precedents[root_key] == upstream_keys, 'Same tasks have to have the same upstream'

        # Group nodes by db_path
        path, arg_key = root_key
        if path not in node_groups:
            node_groups[path] = {arg_key}
        else:
            node_groups[path].add(arg_key)

    stats = {k: len(args) for k, args in node_groups.items()}
    LOGGER.info(f'Following tasks will be called: {stats}')

    # Read concurrency budgets
    budgets: dict[str, int] = {}
    occupied: dict[str, int] = {}
    for path in node_groups:
        mc = task_factories[path].max_concurrency
        if mc is not None:
            budgets[path] = mc
            occupied[path] = 0

    # Collect leaf nodes by groups
    leaves: dict[str, list[Json]] = {
            path: [k for k in keys if not precedents[path, k]]
            for path, keys in node_groups.items()
            }

    # Execute tasks
    with executor as executor:
        in_process: set[Future[Key]] = set()
        while leaves or in_process:
            LOGGER.info(
                    f'desc: {len(descendants)}, prec: {len(precedents)}, leaves: {len(leaves)}, in_process: {len(in_process)}'
                    )

            # Submit all leaf tasks
            leftover: dict[str, list[Json]] = {}
            for path, keys in leaves.items():
                if path in budgets:
                    free = budgets[path] - occupied[path]
                    to_submit, to_hold = keys[:free], keys[free:]
                    occupied[path] += len(to_submit)
                    if to_hold:
                        leftover[path] = to_hold
                else:
                    to_submit = keys

                for key in to_submit:
                    future = executor.submit(_run_task, cloudpickle.dumps(nodes[path, key]))
                    in_process.add(future)

            # Wait for the first tasks to complete
            done, in_process = wait(in_process, return_when=FIRST_COMPLETED)

            # Update graph
            leaves = leftover
            for done_future in done:
                done_task = done_future.result()

                # Update occupied
                path = done_task[0]
                if path in occupied:
                    occupied[path] -= 1
                    assert occupied[path] >= 0

                # Remove node from graph
                nodes.pop(done_task)
                assert not precedents.pop(done_task)
                next_tasks = descendants.pop(done_task)

                # update precedents and leaves
                for next_task in next_tasks:
                    precs = precedents[next_task]
                    precs.remove(done_task)
                    if not precs:
                        path_next, key_next = next_task
                        if path_next not in leaves:
                            leaves[path_next] = [key_next]
                        else:
                            leaves[path_next].append(key_next)

    # Sanity check
    assert not nodes and not descendants and not precedents, f'Graph is not empty. Should not happen.'
    assert all(n == 0 for n in occupied.values()), 'Incorrect task count. Should not happen.'
    return {'stats': stats}


def _run_task(task_data: bytes) -> tuple[str, Json]:
    task = cloudpickle.loads(task_data)
    assert isinstance(task, Task)
    task.set_result()
    return task.to_tuple()
