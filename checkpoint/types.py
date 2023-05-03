from __future__ import annotations
from datetime import datetime
from typing import Any, NewType, ParamSpec, Protocol, Type, TypeVar, runtime_checkable, Callable
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Executor
from pathlib import Path


P = ParamSpec('P')
R = TypeVar('R', covariant=True)
T = TypeVar('T')
Json = NewType('Json', str)
TaskKey = tuple[str, Json]
Runner = Callable[[], R]  # Delayed computation
RunnerFactory = Callable[P, Runner[R]]



class Context:
    cache_dir = Path(os.getenv('CP_CACHE_DIR', './.cache'))
    executor_name = os.getenv('CP_EXECUTOR', 'process')
    max_workers = int(os.getenv('CP_MAX_WORKERS', -1))
    detect_source_change = bool(os.getenv('CP_DETECT_SOURCE_CHANGE', 0))
    num_cpu = os.cpu_count()

    @classmethod
    def get_executor(cls, max_workers: int | None = None) -> Executor:
        if cls.executor_name == 'process':
            executor_type = ProcessPoolExecutor
        elif cls.executor_name == 'thread':
            executor_type = ThreadPoolExecutor
        else:
            raise ValueError('Unrecognized executor name:', cls.executor_name)
        if max_workers is None:
            max_workers = cls.max_workers
        if max_workers < 0:
            assert isinstance(cls.num_cpu, int)
            assert -cls.num_cpu <= cls.max_workers
            max_workers = cls.num_cpu + 1 + cls.max_workers
        return executor_type(max_workers=max_workers)
