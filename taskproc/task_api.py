""" Test module for pickling class-decorated class """

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TypeVar, final
from typing_extensions import ParamSpec, Protocol


P = ParamSpec('P')
R = TypeVar('R', covariant=True)


class TaskProtocol(Protocol[P, R]):
    def init_task(self, *args: P.args, **kwargs: P.kwargs) -> None:
        ...

    def run_task(self) -> R:
        ...


class TaskAPI:
    @final
    def task_directory(self) -> Path:
        ...



class Foo(TaskAPI):
    def task_directory(self) -> Path:
        return Path('./')
