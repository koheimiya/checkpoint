from __future__ import annotations
from collections import defaultdict
from itertools import repeat
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, ClassVar, Generic, NewType, TypeVar, Any, cast, overload
from typing_extensions import ParamSpec, Concatenate, Self
from graphlib import TopologicalSorter
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
        data = self._dumps(obj)
        with self.result_cache as ref:
            ref[key] = data

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


TaskKey = tuple[str, Json]


@dataclass(frozen=True)
class DummyTask(Generic[R]):
    task_factory: TaskFactory[..., R]
    key: Json

    register: ClassVar[dict[TaskKey, AnyTask]] = dict()

    def to_tuple(self) -> TaskKey:
        return self.task_factory.get_db_path(), self.key

    def get_result(self) -> R:
        db = self.task_factory.db
        return db.load(self.key)

    def clear(self) -> None:
        self.register.clear()
        db = self.task_factory.db
        db.delete(self.key)

    def create_task(self, producer: Callable[[], tuple[Runner[R], datetime | None]]) -> Task[R]:
        key = self.to_tuple()
        orig = self.register.get(key, None)
        if orig is None:
            runner, timestamp = producer()
            task = Task(task_factory=self.task_factory, key=self.key, runner=runner, timestamp=timestamp)
            self.register[key] = task
        else:
            task = Task(task_factory=self.task_factory, key=self.key, runner=orig.runner, timestamp=orig.timestamp)
        return task


@dataclass(frozen=True)
class Task(DummyTask[R]):
    """ Runner with cache """
    runner: Runner[R]
    timestamp: datetime | None

    def set_result(self) -> None:
        db = self.task_factory.db
        out = self.runner()
        db.save(self.key, out)

    def run(self, *, executor: Executor | None = None) -> R:
        return self.run_with_info(executor=executor)[0]

    def run_with_info(self, *, executor: Executor | None = None, dump_generations: bool = False) -> tuple[R, dict[str, Any]]:
        # graph = Graph.build(self)
        DummyTask.register.clear()
        graph = Graph.build_from(self)
        graph.prune()
        if executor is None:
            executor = ProcessPoolExecutor()
        # info = run_task_graph(graph=graph, executor=executor, dump_generations=dump_generations)
        info = run_task_graph(graph=graph, executor=executor, dump_generations=dump_generations)
        return self.get_result(), info


@dataclass
class TaskFactory(Generic[P, R]):
    runner_factory: RunnerFactory[P, R]
    db: Database
    max_concurrency: int | None

    def get_db_path(self) -> str:
        return self.db.path

    def clear(self) -> None:
        DummyTask.register.clear()
        self.db.clear()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Task[R]:
        key = _serialize_arguments(self.runner_factory, *args, **kwargs)
        dummy = DummyTask(task_factory=self, key=key)

        def producer():
            runner = self.runner_factory(*args, **kwargs)
            try:
                timestamp = self.db.load_timestamp(key)
            except KeyError:
                timestamp = None
            return runner, timestamp

        return dummy.create_task(producer=producer)


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
    return cast(Json, json.dumps(arguments, separators=(',', ':'), sort_keys=True))


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


def get_precedents(task: AnyTask) -> list[AnyTask]:
    deps: list[Task[Any]] = []
    task_fn = task.runner
    while isinstance(task_fn, (Connected, ListConnected, DictConnected)):
        deps.extend(task_fn.get_tasks())
        task_fn = task_fn.fn
    return deps


@dataclass
class Graph:
    nodes: dict[TaskKey, AnyTask]
    succs: dict[TaskKey, set[TaskKey]]
    preds: dict[TaskKey, set[TaskKey]]

    @classmethod
    def build_from(cls, root: AnyTask) -> Self:
        nodes: dict[TaskKey, AnyTask] = {}
        successors: dict[TaskKey, set[TaskKey]] = defaultdict(set)
        predecessors: dict[TaskKey, set[TaskKey]] = {}

        to_expand: list[tuple[AnyTask, TaskKey | None]] = [(root, None)]
        while to_expand:
            x, desc_key = to_expand.pop()
            key = x.to_tuple()

            if desc_key is None:
                successors[key] = set()
            else:
                successors[key].add(desc_key)

            if key not in predecessors:
                nodes[key] = x
                ps = get_precedents(x)
                predecessors[key] = set(p.to_tuple() for p in ps)
                to_expand.extend(zip(ps, repeat(key)))
            else:
                pass # Already seen x, skip registration/expansion

        return Graph(nodes=nodes, succs=dict(successors), preds=predecessors)

    def prune(self) -> list[AnyTask]:
        """ Prune fresh tasks and return pruned tasks """
        topological_order = tuple(TopologicalSorter(self.succs).static_order())[::-1]
        fresh_timestamps: dict[TaskKey, datetime | None] = {}
        for key in topological_order:
            ts0 = self.nodes[key].timestamp
            if ts0 is not None:
                for p in self.preds[key]:
                    ts_pred = fresh_timestamps[p]
                    if ts_pred is None or ts_pred > ts0:
                        ts0 = None
                        break
            fresh_timestamps[key] = ts0

        pruned: list[AnyTask] = []
        for key, ts0 in fresh_timestamps.items():
            if ts0 is not None:
                pruned.append(self.nodes[key])
                self.pop_and_new_leaves(key, disallow_non_leaf=False)
        return pruned

    def __len__(self) -> int:
        assert len(self.nodes) == len(self.succs) == len(self.preds)
        return len(self.nodes)

    def get_task_factories(self) -> dict[str, TaskFactory[..., Any]]:
        return dict((path, task.task_factory) for (path, _), task in self.nodes.items())

    def grouped_keys(self) -> dict[str, list[Json]]:
        out: dict[str, list[Json]] = defaultdict(list)
        for path, args in self.nodes:
            out[path].append(args)
        return dict(out)

    def get_leaves(self) -> defaultdict[str, list[TaskKey]]:
        leaves: dict[str, list[TaskKey]] = defaultdict(list)
        for (path, args), ps in self.preds.items():
            if not ps:
                leaves[path].append((path, args))
        return leaves

    def pop_and_new_leaves(self, key: TaskKey, disallow_non_leaf: bool = True) -> defaultdict[str, list[TaskKey]]:
        if disallow_non_leaf:
            assert not self.preds[key]
        self.nodes.pop(key)
        self.preds.pop(key)
        ss = self.succs.pop(key)

        new_leaves: dict[str, list[TaskKey]] = defaultdict(list)
        for next_task in ss:
            ps = self.preds[next_task]
            ps.remove(key)
            if not ps:
                path_next = next_task[0]
                new_leaves[path_next].append(next_task)
        return new_leaves


def run_task_graph(graph: Graph, executor: Executor, dump_generations: bool = False) -> dict[str, Any]:
    """ Consume task graph concurrently.
    """
    g = graph
    grouped_keys = g.grouped_keys()

    stats = {k: len(args) for k, args in grouped_keys.items()}
    LOGGER.info(f'Following tasks will be called: {stats}')
    info = {'stats': stats, 'generations': []}

    # Read concurrency budgets
    budgets: dict[str, int] = {}
    occupied: dict[str, int] = {}
    for path, fac in g.get_task_factories().items():
        mc = fac.max_concurrency
        if mc is not None:
            budgets[path] = mc
            occupied[path] = 0

    # Execute tasks
    standby = g.get_leaves()
    in_process: set[Future[TaskKey]] = set()
    with executor as executor:
        # TODO: Coffman-Graham algorithm?
        # TODO: limit in_process <= max_workers
        while standby or in_process:
            # Log some stats
            LOGGER.info(
                    f'nodes: {len(g.nodes)}, desc: {len(g.succs)}, prec: {len(g.preds)}, standby: {len(standby)}, in_process: {len(in_process)}'
                    )
            if dump_generations:
                info['generations'].append(g.grouped_keys())

            # Submit all leaf tasks
            leftover: dict[str, list[TaskKey]] = {}
            for path, keys in standby.items():
                if path in budgets:
                    free = budgets[path] - occupied[path]
                    to_submit, to_hold = keys[:free], keys[free:]
                    occupied[path] += len(to_submit)
                    if to_hold:
                        leftover[path] = to_hold
                else:
                    to_submit = keys

                for key in to_submit:
                    future = executor.submit(_run_task, cloudpickle.dumps(g.nodes[key]))
                    in_process.add(future)

            # Wait for the first tasks to complete
            done, in_process = wait(in_process, return_when=FIRST_COMPLETED)

            # Update graph
            standby = defaultdict(list, leftover)
            for done_future in done:
                done_task = done_future.result()

                # Update occupied
                path = done_task[0]
                if path in occupied:
                    occupied[path] -= 1
                    assert occupied[path] >= 0

                # Remove node from graph
                new_leaves = g.pop_and_new_leaves(done_task)

                # Update standby
                for k, ls in new_leaves.items():
                    standby[k].extend(ls)

    # Sanity check
    assert len(g) == 0, f'Graph is not empty. Should not happen.'
    assert all(n == 0 for n in occupied.values()), 'Incorrect task count. Should not happen.'
    return info


def _run_task(task_data: bytes) -> tuple[str, Json]:
    task = cloudpickle.loads(task_data)
    assert isinstance(task, Task)
    task.set_result()
    return task.to_tuple()
