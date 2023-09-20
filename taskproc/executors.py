from concurrent.futures import ProcessPoolExecutor, Executor, Future
from typing import TypeVar, ParamSpec, Callable

_T = TypeVar('_T')
_P = ParamSpec('_P')
class LocalExecutor(Executor):
    def submit(self, __fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> Future[_T]:
        result = __fn(*args, **kwargs)
        future = Future[_T]()
        future.set_result(result)
        return future
