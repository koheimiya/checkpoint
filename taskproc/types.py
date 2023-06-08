from __future__ import annotations
from typing import TypeVar, Callable
from typing_extensions import Literal, NewType, ParamSpec
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Executor
from pathlib import Path
import dotenv


P = ParamSpec('P')
R = TypeVar('R', covariant=True)
T = TypeVar('T')
Json = NewType('Json', str)
TaskKey = tuple[str, Json]
Runner = Callable[[], R]  # Delayed computation
RunnerFactory = Callable[P, Runner[R]]



dotenv.load_dotenv()


class Context:
    cache_dir = Path(os.getenv('TP_CACHE_DIR', './.cache'))
    executor_name = os.getenv('TP_EXECUTOR', 'process')
    max_workers = int(os.getenv('TP_MAX_WORKERS', -1))
    detect_source_change = bool(os.getenv('TP_DETECT_SOURCE_CHANGE', 0))
    num_cpu = os.cpu_count()

    @classmethod
    def get_executor(cls, executor_name: Literal['process', 'thread'] | str | None = None, max_workers: int | None = None) -> Executor:
        if executor_name is None:
            executor_name = cls.executor_name

        if executor_name == 'process':
            executor_type = ProcessPoolExecutor
        elif executor_name == 'thread':
            executor_type = ThreadPoolExecutor
        else:
            raise ValueError('Unrecognized executor name:', cls.executor_name)

        if max_workers is None and cls.max_workers > 0:
            max_workers = cls.max_workers

        return executor_type(max_workers=max_workers)
