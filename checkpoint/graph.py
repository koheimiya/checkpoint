from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Generic, Type, TypeVar, Any, cast
from typing_extensions import ParamSpec, Concatenate, Self

import logging
import dill

from concurrent.futures import ProcessPoolExecutor, Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
from functools import wraps
import inspect
import json

from checkpoint.env import CHECKPOINT_PATH


from .base_db import Json
from .diskcache_db import DiskCacheDB2


LOGGER = logging.getLogger(__file__)


K = TypeVar('K')
T = TypeVar('T')
P = ParamSpec('P')
R = TypeVar('R')


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

    def __eq__(self, other) -> bool:
        if not isinstance(other, Task):
            return False
        return self.serialize() == other.serialize()

    def run(self, max_workers: int | None = None) -> T:
        graph = Graph.build(self)
        run_task_graph(graph, max_workers=max_workers)
        return self.get_result()

    def clear(self) -> None:
        db = self.task_factory.db
        db.delete(self.key)

    def serialize(self) -> Json:
        d = {'db_path': self.task_factory.get_db_path(), 'key': json.loads(self.key)}
        return cast(Json, json.dumps(d))


AnyTask = Task[Any]


@dataclass
class TaskFactory(Generic[P, R]):
    runner_factory: RunnerFactory[P, R]
    db: DiskCacheDB2

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

    def __eq__(self, other) -> bool:
        if not isinstance(other, TaskFactory):
            return False
        return (self.get_db_path() == other.get_db_path())


def task(
        compress_level: int | None = None
        ) -> Callable[[RunnerFactory[P, R]], TaskFactory[P, R]]:
    """ Convert a runner factory into a task factory. """
    # TODO: add max_concurrency
    # TODO: allow non-parenthesis decoration

    def decorator(fn: RunnerFactory[P, R]) -> TaskFactory[P, R]:
        db_path = str(CHECKPOINT_PATH / _serialize_function(fn))
        db = DiskCacheDB2.make(name=db_path, compress_level=compress_level)
        return wraps(fn)(
                TaskFactory(runner_factory=fn, db=db)
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


def requires(task: Task[T]) -> Connector[T, P, R]:
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


def _run_task(task_data: bytes) -> Json:
    task = dill.loads(task_data)
    assert isinstance(task, Task)
    task.set_result()
    return task.serialize()


def run_task_graph(graph: Graph, max_workers: int | None = None, executor_type: Type[ProcessPoolExecutor] | Type[ThreadPoolExecutor] = ProcessPoolExecutor) -> None:
    """ Consume task graph concurrently.
    """
    # Parse graph in a flat format
    nodes: dict[Json, AnyTask] = {}
    descendants: dict[Json, set[Json]] = {}
    precedents: dict[Json, set[Json]] = {}
    leaves: set[Json] = set()
    for g in walk_subgraph_to_update(graph):
        root_key = g.root.serialize()
        nodes[root_key] = g.root

        if root_key not in descendants:
            descendants[root_key] = set()
        if g.downstream:
            descendants[root_key].add(g.downstream.serialize())

        if g.upstream_graphs:
            if root_key not in precedents:
                precedents[root_key] = set(ug.root.serialize() for ug in g.upstream_graphs)
            else:
                assert precedents[root_key] == set(ug.root.serialize() for ug in g.upstream_graphs), 'Same tasks have to have the same upstream'
        else:
            leaves.add(root_key)

    with executor_type(max_workers=max_workers) as executor:
        in_process: set[Future[Json]] = set()
        while leaves:
            LOGGER.info(
                    f'desc: {len(descendants)}, prec: {len(precedents)}, leaves: {len(leaves)}, in_process: {len(in_process)}'
                    )

            # Submit all leaf tasks
            for leaf in leaves:
                future = executor.submit(_run_task, dill.dumps(nodes[leaf], protocol=dill.HIGHEST_PROTOCOL, byref=True))
                in_process.add(future)

            # Wait for the first tasks to complete
            done, in_process = wait(in_process, return_when=FIRST_COMPLETED)

            # Update graph
            leaves = set()
            for done_future in done:
                done_task = done_future.result()
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
    return
