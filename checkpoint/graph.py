from __future__ import annotations
from dataclasses import asdict, dataclass
from typing import Callable, Generic, TypeVar, Any, cast
from typing_extensions import ParamSpec, Concatenate, Self

# import dill
# import multiprocessing.reduction
# dill.Pickler.dumps, dill.Pickler.loads = dill.dumps, dill.loads  # type: ignore
# multiprocessing.reduction.ForkingPickler = dill.Pickler
# multiprocessing.reduction.dump = dill.dump

from concurrent.futures import ProcessPoolExecutor, Future, wait, FIRST_COMPLETED, ThreadPoolExecutor
from multiprocessing import get_context
from functools import wraps
import inspect
import json

from checkpoint.env import CHECKPOINT_PATH


from .base_db import Json
from .diskcache_db import DiskCacheDB


T = TypeVar('T')
P = ParamSpec('P')
R = TypeVar('R')


Runner = Callable[[], T]  # Delayed computation
RunnerFactory = Callable[P, Runner[R]]


@dataclass(frozen=True)
class DelayedRunner(Generic[T]):
    """ Delayed call on a runner factory """
    runner_factory: RunnerFactory[..., T]
    arguments: dict[str, Any]
    _runner: Runner[T] | None

    @classmethod
    def make(cls, runner_factory: RunnerFactory[P, T], *args: P.args, **kwargs: P.kwargs) -> Self:
        runner = runner_factory(*args, **kwargs)
        return DelayedRunner(runner_factory=runner_factory, arguments=_normalize_arguments(runner_factory, *args, **kwargs), _runner=runner)

    def without_runner(self) -> Self:
        return DelayedRunner(runner_factory=self.runner_factory, arguments=self.arguments, _runner=None)

    def __call__(self) -> T:
        return self.runner_factory(**self.arguments)()

    def __eq__(self, other) -> bool:
        if not isinstance(other, DelayedRunner):
            return False
        return (self.runner_factory is other.runner_factory) and (self.arguments == other.arguments)


@dataclass(frozen=True)
class Task(Generic[T]):
    """ Runner with cache """
    _delayed_runner: DelayedRunner[T]
    task_factory: TaskFactory[..., T]
    key: Json

    def set_result(self) -> None:
        db = self.task_factory.connect_db()
        out = self._delayed_runner()
        db.save(self.key, out)

    def get_result(self) -> T:
        db = self.task_factory.connect_db()
        return db.load(self.key)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Task):
            return False
        return self.serialize() == other.serialize()

    def run(self, **executor_params) -> T:
        graph = Graph.build(self)
        run_task_graph(graph, executor_params=executor_params)
        return self.get_result()

    def clear(self) -> None:
        db = self.task_factory.connect_db()
        db.delete(self.key)

    def serialize(self) -> Json:
        d = {'db_path': self.task_factory.get_db_path(), 'key': json.loads(self.key)}
        return cast(Json, json.dumps(d))

    def without_runner(self) -> Self:
        return Task(_delayed_runner=self._delayed_runner.without_runner(), task_factory=self.task_factory, key=self.key)


AnyTask = Task[Any]


@dataclass
class TaskFactory(Generic[P, R]):
    runner_factory: RunnerFactory[P, R]
    _compress_level: int | None

    def get_db_path(self) -> str:
        return str(CHECKPOINT_PATH / _serialize_function(self.runner_factory))

    def connect_db(self) -> DiskCacheDB:
        return DiskCacheDB.make(name=self.get_db_path(), compress_level=self._compress_level)

    def clear(self) -> None:
        db = self.connect_db()
        db.clear()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Task[R]:
        runner = DelayedRunner.make(self.runner_factory, *args, **kwargs)
        # runner = self.runner_factory(*args, **kwargs)
        key = _serialize_arguments(self.runner_factory, *args, **kwargs)
        return Task(runner, task_factory=self, key=key)

    def __eq__(self, other) -> bool:
        if not isinstance(other, TaskFactory):
            return False
        return (self.get_db_path() == other.get_db_path())


def task(
        compress_level: int | None = None
        ) -> Callable[[RunnerFactory[P, R]], TaskFactory[P, R]]:
    """ Convert a runner factory into a task factory. """

    def decorator(fn: RunnerFactory[P, R]) -> TaskFactory[P, R]:
        return wraps(fn)(
                TaskFactory(runner_factory=fn, _compress_level=compress_level)
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


@dataclass(eq=True, frozen=True)
class Connected(Generic[T, P, R]):
    """ Connect a task to a function. """
    task: Task[T]
    fn: Callable[Concatenate[T, P], R]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        x = self.task.get_result()
        return self.fn(x, *args, **kwargs)


AnyConnected = Connected[Any, P, R]
Connector = Callable[[Callable[Concatenate[T, P], R]], Callable[P, R]]  # Takes (T, *P) -> R and return P -> R


def requires(task: Task[T]) -> Connector[T, P, R]:
    """ Register a task dependency """
    def decorator(fn: Callable[Concatenate[T, P], R]) -> Callable[P, R]:
        return Connected(task, fn)
    return decorator


def get_upstream(task: Task[Any]) -> list[Task[Any]]:
    deps: list[Task[Any]] = []
    task_fn = task._delayed_runner._runner
    while isinstance(task_fn, Connected):
        deps.append(task_fn.task)
        task_fn = task_fn.fn
    return deps


@dataclass
class Graph:
    root: Task[Any]
    downstream: Task[Any] | None
    upstream_graphs: list[Graph]

    @classmethod
    def build(cls, task: Task[Any], downstream: Task[Any] | None = None) -> Self:
        upstream_tasks = get_upstream(task)
        out = Graph(
                root=task,
                downstream=downstream,
                upstream_graphs=[Graph.build(t, downstream=task) for t in upstream_tasks]
                )
        return out


def _run_task(task: AnyTask) -> Json:
    task.set_result()  # TODO: use time stamp
    return task.serialize()


def run_task_graph(graph: Graph, executor_params: dict[str, Any] | None = None) -> None:
    """ Run task graph with concurrently.
    """
    if executor_params is None:
        executor_params = {}

    # Parse graph in a flat format
    nodes: dict[Json, AnyTask] = {}
    descentdants: dict[Json, set[Json]] = {}
    precedents: dict[Json, set[Json]] = {}
    leaves: set[Json] = set()
    for g in walk_graph(graph):
        root_key = g.root.serialize()
        nodes[root_key] = g.root.without_runner()
        if root_key not in descentdants:
            descentdants[root_key] = set()
        if g.downstream:
            descentdants[root_key].add(g.downstream.serialize())
        if g.upstream_graphs:
            if root_key not in precedents:
                precedents[root_key] = set(ug.root.serialize() for ug in g.upstream_graphs)
            else:
                assert precedents[root_key] == set(ug.root.serialize() for ug in g.upstream_graphs), 'Same tasks have to have the same upstream'
        else:
            leaves.add(root_key)

    with ProcessPoolExecutor(**executor_params) as executor:
    # with ThreadPoolExecutor(**executor_params) as executor:
        in_process: set[Future[Json]] = set()
        while descentdants:
            print(len(descentdants), len(precedents), len(leaves), len(in_process))
            # breakpoint()

            # Submit all leaf tasks
            for leaf in leaves:
                future = executor.submit(_run_task, nodes[leaf])
                in_process.add(future)

            # Wait for the first tasks to complete
            done, in_process = wait(in_process, return_when=FIRST_COMPLETED)

            # Update graph
            leaves = set()
            for done_future in done:
                done_task = done_future.result()
                nodes.pop(done_task)
                next_tasks = descentdants.pop(done_task)
                for next_task in next_tasks:
                    if next_task is None:
                        continue
                    deps = precedents[next_task]
                    deps.remove(done_task)
                    if not deps:
                        leaves.add(next_task)
                        precedents.pop(next_task)

    # Confirm the graph is empty
    assert not descentdants, 'Something went wrong'
    assert not precedents, 'Something went wrong'
    assert not leaves, 'Something went wrong'
    assert not in_process, 'Something went wrong'
    return


def walk_graph(graph: Graph) -> list[Graph]:
    out: list[Graph] = [graph]
    to_expand: list[Graph] = graph.upstream_graphs.copy()
    while to_expand:
        g = to_expand.pop()
        out.append(g)
        to_expand.extend(g.upstream_graphs)
    return out
