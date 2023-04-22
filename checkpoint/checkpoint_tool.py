from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, TypeVar
from typing_extensions import ParamSpec
from functools import wraps


from .diskcache_db import DiskCacheDB
from .function import FunctionWithDB


T = TypeVar('T')
P = ParamSpec('P')


@dataclass
class checkpoint:
    """ Decorator for persistent cache.
    The arguments of the function must be json-compatible.
    Values are stored in `$CHECKPOINT_PATH/.checkpoints/{name}.sqlite`. `$CHECKPOINT_PATH` defaults to the current working directory.
    """
    name: str | None = None
    compress_level: int | None = None

    def __call__(self, func: Callable[P, T]) -> FunctionWithDB[P, T]:
        db_factory = lambda name: DiskCacheDB.make(name=name, compress_level=self.compress_level)
        return wraps(func)(FunctionWithDB.make(name=self.name, func=func, db_factory=db_factory))
