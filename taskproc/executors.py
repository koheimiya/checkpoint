from concurrent.futures import ProcessPoolExecutor, Executor, Future
from typing import TypeVar, ParamSpec, Callable
import logging


LOGGER = logging.getLogger(__name__)


_T = TypeVar('_T')
_P = ParamSpec('_P')
class LocalExecutor(Executor):
    def __init__(self, max_workers: int | None = None) -> None:
        if max_workers is not None and max_workers > 1:
            LOGGER.warn(f'{max_workers=} is passed to LocalExecutor. Ignored.')

    def submit(self, __fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> Future[_T]:
        result = __fn(*args, **kwargs)
        future = Future[_T]()
        future.set_result(result)
        return future
