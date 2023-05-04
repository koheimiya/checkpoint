# Checkpoint-tool

A lightweight workflow building/execution/management tool written in pure Python.

Internally, it depends on `DiskCache`, `cloudpickle` `networkx` and `concurrent.futures`.


## Installation

```
pip install checkpoint-tool
```

## Usage

### Basic usage

Workflow is a directed acyclic graph (DAG) of tasks, and task is a unit of work represented with a nested function.
Here is an example.
```python
from checkpoint import Task, Req, Requires, Const

# Define a task and **its entire upstream workflow** with a class definition.
# Inheriting `Task` is necesary, as it takes care of all the work storing and reusing the result and tracking the dependencies.
# `infer_task_type` decorator helps the type checker to infer the types of the task class. (optional)
@infer_task_type
class Choose(Task):
    """ Compute the binomial coefficient. """
    # Inside a task, we first declare the prerequisite tasks with the descriptor `Req`.
    # In this example, `Choose(n, k)` depends on `Choose(n - 1, k - 1)` and `Choose(n - 1, k)`,
    # so we have two prerequisite tasks computing `int` values.
    # Through these requirements, we recursively defines all the tasks we need to compute this task,
    # i.e., the entire upstream workflow.
    # The type hint with `Requires` is optional.
    prev1: Requires[int] = Req()
    prev2: Requires[int] = Req()

    def init(self, n: int, k: int):
        # The prerequisite tasks and other instance attributes are prepared here.
        # This method is optional if we do not have prerequisite tasks.
        if 0 < k < n:
            self.prev1 = Choose(n - 1, k - 1)
            self.prev2 = Choose(n - 1, k)
        elif k == 0 or k == n:
            # We can just pass a value to a requirement slot directly without running tasks.
            self.prev1 = Const(0)
            self.prev2 = Const(1)
        else:
            raise ValueError(f'{(n, k)}')

    def main(self) -> int:
        # It is mandatory to implement this method.
        # Here we define the main computation of the task,
        # which is delayed until it is necessary.
        # The return values of the preerquisite tasks are accessible via the descriptors:
        return self.prev1 + self.prev2

# To run tasks, use the `run()` method.
ans = Choose(6, 3).run()  # `ans` should be 6 Choose 3, which is 20.

# It greedily executes all the necessary tasks as parallel as possible
# and then spits out the return value of the task on which we call `run()`.
# The return values of the intermediate tasks are cached at
# `{$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.{function_name}/...`
# and reused on the fly whenever possible.
```

### Deleting cache

It is possible to selectively discard cache: 
```python
# After some modificaiton of `Choose(3, 3)`,
# selectively discard the cache corresponding to the modification.
Choose(3, 3).clear()

# `ans` is recomputed tracing back to the computation of `Choose(3, 3)`.
ans = Choose(6, 3).run()

# Delete all the cache associated with `Choose`,
# equivalent to `rm -r {$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.Choose`.
Choose.clear_all()            
```

### Advanced IO

Tasks can be initialized with any arguments as long as it is JSON serializable:
```python
class t1(Task):
    def init(self, **param1):
        ...

class t2(Task):
    def init(self, **param2):
        ...

class t3(Task):
    x1 = Req()
    x2 = Req()

    def init(self, json_params):
        self.x1 = t1(**json_params['param1'])
        self.x2 = t2(**json_params['param2'])

    def main(self):
        ...

result = t3({'param1': { ... }, 'param2': { ... }}).run()
```

Even more complex inputs can be passed directly as `Task`s:
```python
Dataset = ...  # Some complex data structure
Model = ...    # Some complex data structure

class load_dataset(Task):
    def main(self) -> Dataset:
        ...

class train_model(Task):
    dataset = Req()

    def init(self, dataset_task: Task[Dataset]):
        self.dataset = dataset_task

    def main(self) -> Model:
        ...
    
class score_model(Task):
    dataset = Req()
    model = Req()

    def init(self, dataset_task: Task[Dataset], model_task: Task[Model]):
        self.dataset = dataset_task
        self.model = model_task

    def main(self) -> float:
        ...


dataset_task = load_dataset()
model_task = train_model(dataset)
score_task = score_model(dataset, model)
print(score_task.run())
```

Task dependencies can be specified with lists and dicts:
```python
from checkpoint import RequiresDict


class summarize_scores(Task):
    scores: RequiresDict[str, float] = Req()  # Again, type annotation is optional.

    def init(self, scores_tasks: dict[str, Task[float]]):
        self.scores = scores_tasks

    def main(self):
        return sum(self.scores.values()) / len(self.scores)  # We have access to the dict of the results.
```

Large outputs can be stored with compression via `zlib`:
```python
class large_output_task(Task, compress_level=-1):
    ...
```

### Data directories

Use `Task.directory` as a fresh directory dedicated to each task.
A directory is automatically created at
`{$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.{task_name}/data/{cryptic_task_id}`
and the contents of the directory are cleared at each task call and persist until the task is `clear`ed.
```python
from pathlib import Path
from checkpoint import TaskDirectory


class train_model(Task):

    def main(self) -> str:
        ...
        model_path = str(self.directory / 'model.bin')
        model.save(model_path)
        return model_path
```

### Execution policy configuration

One can control the task execution with `concurrent.futures.Executor` class:
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class my_task(Task):
    ...

# Limit the number of parallel workers
my_task().run(executor=ProcessPoolExecutor(max_workers=2))

# Thread-based parallelism
my_task().run(executor=ThreadPoolExecutor())
```

One can also control the concurrency at a task/queue level:
```python
class task_using_gpu(Task, queue='gpu'):
    ...

class another_task_using_gpu(Task, queue='gpu'):
    ...

some_downstream_task.run(rate_limits={'gpu': 1})  # Queue-level concurrency control
some_downstream_task.run(rate_limits={yet_another_task.queue: 1})  # Task-level concurrency control

```

### Commandline tool
We can use checkpoint-tool from commandline like `python -m checkpoint path/to/taskfile.py`, where `taskfile.py` defines the `main` task as follows:
```python
# taskfile.py

class Main(Task):
    ...
```
The command runs the `Main()` task and stores the cache right next to `taskfile.py` as `.cache/checkpoint/...`.
Please refer to `python -m checkpoint --help` for more info.



## TODO
 - [ ] Taskfile examples for class-decorator based implementation
 - [ ] Simple visualizers
    - [ ] Task-wise progressbar
    - [ ] Graph visualizer
