# Checkpoint-tool

A lightweight workflow management tool written in pure Python.

Internally, it depends on `DiskCache`, `cloudpickle` `networkx` and `concurrent.futures`.


## Installation

```
pip install checkpoint-tool
```

## Usage

### Basic usage

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
        def __(prev1: int, prev2: int) -> int:
            # Main computation
            return prev1 + prev2
    elif k == 0 or k == n:
        # Dependency can change according to the task parameters (`n` and `k`).
        # Here, we need no dependency to compute `choose(n, 1)` or `choose(n, n)`.
        def __() -> int:
            return 1
    else:
        raise ValueError(f'{(n, k)}')

    # Return function that produces a value instead of the value itself.
    return __

# Build the task graph to compute `choose(6, 3)`
# and greedily consume it with `concurrent.futures.ProcessPoolExecutor`
# (i.e., as parallel as possible).
# The cache is stored at `{$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.{function_name}/...`
# and reused whenever available.
ans = choose(6, 3).run()
```

### Deleting cache

It is possible to selectively discard cache: 
```python
# After some modificaiton of `choose(3, 3)`,
# selectively discard the cache corresponding to the modification.
choose(3, 3).clear()

# `ans` is recomputed tracing back to the computation of `choose(3, 3)`.
ans = choose(6, 3).run()

# Delete all the cache associated with `choose`,
# equivalent to `rm -r {$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.choose`.
choose.clear()            
```

### Advanced IO

More complex inputs can be passed as long as it is JSON serializable:
```python
@task
def f1(**param1):
    ...

@task
def f2(**param2):
    ...

@task
def f3(json_params):
    @requires(f1(**json_params['param1']))
    @requires(f2(**json_params['param2']))
    def __(obj1, obj2):
        ...
    return __

result = f3({'param1': { ... }, 'param2': { ... }}).run()
```

Even more complex inputs can be passed as `task`s:
```python
from checkpoint import Task

Dataset = ...  # Some complex data structure
Model = ...    # Some complex data structure

@task
def load_dataset():
    def __() -> Dataset:
        ...
    return __

@task
def train_model(dataset_task: Task[Dataset]):
    @requires(dataset_task)
    def __(dataset) -> Model:
        ...
    return __
    
@task
def score_model(dataset_task: Task[Dataset], model_task: Task[Model]):
    @requires(dataset_task)
    @requires(model_task)
    def __(dataset, model) -> float:
        ...
    return __


dataset_task = load_dataset()
model_task = train_model(dataset)
score_task = score_model(dataset, model)
print(score_task.run())
```

Task dependencies can be specified with lists and dicts:
```python
@task
def summarize_scores(scores_tasks: dict[str, Task[float]]):

    @requires(score_tasks)
    def __(score_dict):
        ...
    return __
```

Large outputs can be stored with compression via `zlib`:
```python
@task(compress_level=-1)
def large_output_task():
    ...
```

### Data directories

Use `TaskDirectory` to create a fresh directory dedicated to each task. The contents of the directory are cleared at each task call and persist until the task is `clear`ed.
```python
from pathlib import Path
from checkpoint import TaskDirectory

@task
def train_model(...):

    # Passing a new directory at
    # `{$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.{function_name}/data/{cryptic_task_id}`
    @requires(TaskDirectory())
    def __(path: Path) -> str:
        ...
        model_path = str(path / 'model.bin')
        model.save(model_path)
        return model_path

    return __
```

### Execution policy configuration

One can control the task execution with `concurrent.futures.Executor` class:
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

@task
def my_task():
    ...

# Limit the number of parallel workers
my_task().run(executor=ProcessPoolExecutor(max_workers=2))

# Thread-based parallelism
my_task().run(executor=ThreadPoolExecutor())
```

One can also control the concurrency at a task/queue level:
```python
# Task-level concurrency limit
my_task().run(rate_limit={my_task.queue: 2})

# Queue-level concurrency limit
@task(queue='gpu')
def task_using_gpu():
    ...

@task(queue='gpu')
def another_task_using_gpu():
    ...

# ...
some_downstream_task.run(rate_limit={'gpu': 1})

```

### Commandline tool
We can use checkpoint-tool from commandline like `python -m checkpoint path/to/taskfile.py`, where `taskfile.py` defines the `main` task as follows:
```python
# taskfile.py

@task
def main():
    ...
```
The command runs the `main` task and stores the cache right next to `taskfile.py` as `.cache/checkpoint/...`.
Please refer to `python -m checkpoint --help` for more info.
