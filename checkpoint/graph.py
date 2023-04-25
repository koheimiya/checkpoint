from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Generic, TypeVar, Any, cast
from typing_extensions import ParamSpec, Concatenate, Self
from functools import wraps
import inspect
import json


from .base_db import DBProtocol, Json
from .diskcache_db import DiskCacheDB


T = TypeVar('T')
P = ParamSpec('P')
R = TypeVar('R')


Runner = Callable[[], T]
RunnerFn = Callable[P, Runner[R]]


@dataclass
class Task(Generic[T]):
    runner: Runner[T]
    db: DBProtocol
    key: Json

    def set_result(self) -> None:
        out = self.runner()
        self.db.save(self.key, out)

    def get_result(self) -> T:
        return self.db.load(self.key)


TaskFn = Callable[P, Task[R]]


def task(
        compress_level: int | None = None
        ) -> Callable[[RunnerFn[P, R]], TaskFn[P, R]]:
    """ Insert cache to task runner factory. """
    db_factory = lambda fn: DiskCacheDB.make(_serialize_function(fn), compress_level=compress_level)

    def wrapper(fn: RunnerFn[P, R]) -> TaskFn[P, R]:
        db = db_factory(fn)

        @wraps(fn)
        def inner(*args: P.args, **kwargs: P.kwargs) -> Task[R]:
            key = _serialize_arguments(fn, *args, **kwargs)
            task = fn(*args, **kwargs)
            return Task(task, db, key)
        return inner
    return wrapper


def _serialize_function(fn: Callable[..., Any]) -> str:
    return f'{fn.__module__}.{fn.__qualname__}'


def _serialize_arguments(fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> Json:
    params = inspect.signature(fn).bind(*args, **kwargs)
    params.apply_defaults()
    arguments = params.arguments
    return cast(Json, json.dumps(arguments))


@dataclass
class Peeled(Generic[T, P, R]):
    task0: Task[T]
    unpeel: Callable[Concatenate[T, P], R]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        x = self.task0.get_result()
        return self.unpeel(x, *args, **kwargs)


AnyPeeled = Peeled[Any, P, R]
Peeler = Callable[[Callable[Concatenate[T, P], R]], AnyPeeled[P, R]]  # Takes (T, *P) -> R and return P -> R


def requires(task: Task[T]) -> Peeler[T, P, R]:
    """ Register a task dependency """
    def wrapper(runner: Callable[Concatenate[T, P], R]) -> AnyPeeled[P, R]:
        return Peeled(task, runner)
    return wrapper


def get_upstream(task: Task[Any]) -> list[Task[Any]]:
    deps: list[Task[Any]] = []
    runner = task.runner
    while isinstance(runner, Peeled):
        deps.append(runner.task0)
        runner = runner.unpeel
    return deps


@dataclass
class Graph:
    root: Task[Any]
    children: list[Graph]

    @classmethod
    def build(cls, task: Task[Any]) -> Self:
        upstream = get_upstream(task)
        out = Graph(
                root=task,
                children=[Graph.build(t) for t in upstream]
                )
        return out

    def dispatch(self) -> None:
        ...  # TODO: implement here


def run(task: Task[T]) -> T:
    graph = Graph.build(task)
    graph.dispatch()
    return task.get_result()


@task()
def example_fetch_data(s: str) -> Runner[int]:
    def runner():
        return len(s)
    return runner


@task()
def example_add(s: str, b: int) -> Runner[int]:
    @requires(example_fetch_data(s))
    def runner(data: int) -> int:
        return data + b
    return runner
