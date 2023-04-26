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

@task  # Mark a function as task
def choose(n: int, k: int):
    if 0 < k < n:
        @requires([choose(n - 1, k - 1), choose(n - 1, k)])  # Dependency
        def run_task(prev_two: list[int]):
            return sum(prev_two)
    elif k == 0 or k == n:
        def run_task() -> int:
            return 1
    else:
        raise ValueError(f'{(n, k)}')
    return run_task

# Build the task graph to compute the return value of choose(6, 3) and consume it in parallel whenever possible.
# The cache is stored at `$CP_CACHE_DIR/checkpoint/{module_name}.choice/...` and reused whenever available.
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

Large outputs can be stored with compression:
```python
@task(compress=True)
def large_output_task(*args, **kwargs):
    ...
```

One can limit the task execution with `concurrent.futures.Executor`:
```python
from concurrent.futures import ProcessPoolExecutor

@task
def my_task():
    ...

my_task().run(executor=ProcessPoolExecutor(max_workers=2))
```

One can also control the task-wise concurrency:
```python
@task(max_concurrency=2)
def resource_intensive_task(*args, **kwargs):
    ...
```
