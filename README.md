# Checkpoint-tool
A lightweight workflow management tool written in pure Python.

Internally, it depends on `DiskCache`, `dill` and `concurrent.futures`.


### Installation
```
pip install checkpoint-tool
```

### Usage
Create task with decorators:
```python
from checkpoint import task, requires

# Mark a function as task
@task
def choose(n: int, k: int):
    if 0 < k < n:
        # Mark dependencies on other tasks;
        # The return values of these tasks are passed as the arguments.
        @requires(choose(n - 1, k - 1))
        @requires(choose(n - 1, k)) 
        def run_task(prev1: int, prev2: int) -> int:
            # Main computation
            return prev1 + prev2
    elif k == 0 or k == n:
        # Dependency can change according to the task parameters (`n` and `k`).
        # Here, we need no dependency to compute `choose(n, 1)` or `choose(n, n)`.
        def run_task() -> int:
            return 1
    else:
        raise ValueError(f'{(n, k)}')
    return run_task

# Build the task graph to compute the return value of choose(6, 3)
# and greedily consume it with `concurrent.futures.ProcessPoolExecutor` (i.e., in parallel as far as possible).
# The cache is stored at `$CP_CACHE_DIR/checkpoint/{module_name}.choice/...`
# and reused whenever available.
ans = choose(6, 3).run()
```

It is possible to selectively discard cache: 
```python
### after some modificaiton of choose(3, 3) ...
choose(3, 3).clear()      # selectively discard the cache corresponding to the modification
ans = choose(6, 3).run()  # ans is recomputed tracing back to the computation of choose(3, 3)
choose.clear()            # delete all cache
```

More complex inputs can be used as long as it is JSON serializable:
```python
@task
def task1(**param1):
    ...

@task
def task2(**param2):
    ...

@task
def task3(params):
    @requires(task1(**params['param1']))
    @requires(task2(**params['param2']))
    def run_task(result1, result2):
        ...
    return run_task

result = task3({'param1': { ... }, 'param2': { ... }}).run()
```

Task dependencies can be also specified with lists and dicts.
```python
@task
def task3(params):
    @requires([task1(p) for p in params['my_param_list']])
    @requires({k: task2(p) for k, p in params['my_param_dict'].items()})
    def run_task(result_list, result_dict):
        ...
    return run_task

result = task3({'my_param_list': [ ... ], 'my_param_dict': { ... }}).run()
```

Large outputs can be stored with compression via `zlib`:
```python
@task(compress_level=6)
def large_output_task(*args, **kwargs):
    ...
```

One can control the task execution with `concurrent.futures.Executor` class:
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

@task
def my_task():
    ...

my_task().run(executor=ProcessPoolExecutor(max_workers=2))  # Limit the number of parallel workers
my_task().run(executor=ThreadPoolExecutor())                # Thread-based parallelism
```

One can also control the concurrency at a task level:
```python
@task(max_concurrency=2)
def resource_intensive_task(*args, **kwargs):
    ...
```
