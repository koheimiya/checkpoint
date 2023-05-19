# Checkpoint-tool

A lightweight workflow building/execution/management tool written in pure Python.

Internally, it depends on `DiskCache`, `cloudpickle` `networkx` and `concurrent.futures`.


## Installation

```
pip install checkpoint-tool
```

## Usage

### Basic usage

Workflow is a directed acyclic graph (DAG) of tasks, and task is a unit of work represented with a class.
Here is an example.
```python
from checkpoint import TaskBase, Req, Requires, Const

# Define a task and **its entire upstream workflow** with a class definition.
# Inheriting `TaskBase` is necesary, as it takes care of all the work storing and reusing the result and tracking the dependencies.
# `infer_task_type` decorator helps the type checker to infer the types of the task class. (optional)
@infer_task_type
class Choose(TaskBase):
    """ Compute the binomial coefficient. """
    # Inside a task, we first declare the values that must be computed upstream with the descriptor `Req`.
    # In this example, `Choose(n, k)` depends on `Choose(n - 1, k - 1)` and `Choose(n - 1, k)`,
    # so it requires two `int` values.
    # Either the type annotation `Requires[...]` or the assignment `= Req()` may be omitted.
    prev1: Requires[int] = Req()
    prev2: Requires[int] = Req()

    def build_task(self, n: int, k: int):
        # The prerequisite tasks and the other instance attributes are prepared here.
        # It thus recursively defines all the tasks we need to run this task,
        # i.e., the entire upstream workflow.

        if 0 < k < n:
            self.prev1 = Choose(n - 1, k - 1)
            self.prev2 = Choose(n - 1, k)
        elif k == 0 or k == n:
            # We can just pass a value to a requirement slot directly without running tasks.
            self.prev1 = Const(0)
            self.prev2 = Const(1)
        else:
            raise ValueError(f'{(n, k)}')

    def run_task(self) -> int:
        # Here we define the main computation of the task,
        # which is delayed until it is necessary.

        # The return values of the prerequisite tasks are accessible via the descriptors:
        return self.prev1 + self.prev2

# To run the task as well as upstream workflow, use the `run_graph()` method.
ans = Choose(6, 3).run_graph()  # `ans` should be 6 Choose 3, which is 20.

# It greedily executes all the necessary tasks as parallel as possible
# and then spits out the return value of the task on which we call `run_graph()`.
# The return values of the intermediate tasks are cached at
# `{$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.{task_name}/...`
# and reused on the fly whenever possible.
```

### Deleting cache

It is possible to selectively discard cache: 
```python
# After some modificaiton of `Choose(3, 3)`,
# selectively discard the cache corresponding to the modification.
Choose(3, 3).clear_task()

# `ans` is recomputed tracing back to the computation of `Choose(3, 3)`.
ans = Choose(6, 3).run_graph()

# Delete all the cache associated with `Choose`,
# equivalent to `rm -r {$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.Choose`.
Choose.clear_all_tasks()            
```

### Task IO

The arguments of the `init` method can be anything JSON serializable:
```python
class T1(TaskBase):
    def build_task(self, **param1):
        ...
    ...

class T2(TaskBase):
    def build_task(self, **param2):
        ...
    ...

class T3(TaskBase):
    x1 = Req()
    x2 = Req()

    def build_task(self, json_params):
        self.x1 = T1(**json_params['param1'])
        self.x2 = T2(**json_params['param2'])

    def run_task(self):
        ...

result = T3({'param1': { ... }, 'param2': { ... }}).run_graph()
```

Otherwise they can be passed via `Task` and `Req`:
```python
from checkpoint import Task
Dataset = ...  # Some complex data structure
Model = ...    # Some complex data structure

class LoadDataset(TaskBase):
    def build_task(self):
        pass

    def run_task(self) -> Dataset:
        ...

class TrainModel(TaskBase):
    dataset: Requires[Datset]

    def build_task(self, dataset_task: Task[Dataset]):
        self.dataset = dataset_task

    def run_task(self) -> Model:
        ...
    
class ScoreModel(TaskBase):
    dataset: Requires[Datset]
    model: Requires[Model]

    def build_task(self, dataset_task: Task[Dataset], model_task: Task[Model]):
        self.dataset = dataset_task
        self.model = model_task

    def run_task(self) -> float:
        ...


dataset_task = LoadDataset()
model_task = TrainModel(dataset)
score_task = ScoreModel(dataset, model)
print(score_task.run_graph()
```

`Req` accepts a list/dict of tasks and automatically unfolds it.
```python
from checkpoint import RequiresDict


class SummarizeScores(TaskBase):
    scores: RequiresDict[str, float] = Req()  # Again, type annotation or assignment may be omitted.

    def build_task(self, task_dict: dict[str, Task[float]]):
        self.scores = task_dict

    def run_task(self) -> float:
        return sum(self.scores.values()) / len(self.scores)  # We have access to the dict of the results.
```

One can also directly access the items of dictionary-valued upstream tasks.
```python
class MultiOutputTask(TaskBase):
    ...

    def run_task(self) -> dict[str, int]:
        return {'foo': 42, ...}

class DownstreamTask(TaskBase):
    dep: Requires[int]

    def build_task(self):
        self.dep = MultiOutputTask()['foo']
```

The output of the `run_task` method should be serializable with `cloudpickle`,
which is then compressed with `gzip`.
The compression level can be changed as follows (defaults to 9).
```python
class NoCompressionTask(TaskBase, compress_level=0):
    ...
```

### Job scheduling and prefixes
To run task on job schedulers, one can add prefix to the call of task.
```python

class TaskWithJobScheduler(TaskBase, job_prefix=['jbsub', '-tty', '-queue x86_1h', '-cores 16+1', '-mem 64g', '-require a100_80gb']):
    ...
```

### Data directories

Use `task.task_directory` to get a fresh path dedicated to each task.
The directory is automatically created at
`{$CP_CACHE_DIR:-./.cache}/checkpoint/{module_name}.{task_name}/data/{task_id}`
and the contents of the directory are cleared at each task call and persist until the task is cleared.
```python
class TrainModel(TaskBase):
    ...

    def run_task(self) -> str:
        ...
        model_path = self.task_directory / 'model.bin'
        model.save(model_path)
        return model_path
```

### Execution policy configuration

One can control the task execution with `concurrent.futures.Executor` class:
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class MyTask(TaskBase):
    ...

# Limit the number of parallel workers
MyTask().run_graph(executor=ProcessPoolExecutor(max_workers=2))

# Thread-based parallelism
MyTask().run_graph(executor=ThreadPoolExecutor())
```

One can also control the concurrency at a task/channel level:
```python
class TaskUsingGPU(TaskBase, channel='<gpu>'):
    ...

class AnotherTaskUsingGPU(TaskBase, channel=['<gpu>', '<memory>']):
    ...

# Queue-level concurrency control
SomeDownstreamTask().run_graph(rate_limits={'<gpu>': 1})
SomeDownstreamTask().run_graph(rate_limits={'<memory>': 1})

# Task-level concurrency control
SomeDownstreamTask().run_graph(rate_limits={TaskUsingGPU.task_name: 1})

```

### Commandline tool
We can use checkpoint-tool from commandline like `python -m checkpoint.app path/to/taskfile.py`, where `taskfile.py` defines the `Main` task as follows:
```python
# taskfile.py

class Main(TaskBase):
    ...
```
The command runs the `Main()` task and stores the cache right next to `taskfile.py` as `.cache/checkpoint/...`.
Please refer to `python -m checkpoint.app --help` for more info.

### Other useful properties
* `TaskBase.task_id`
* `TaskBase.task_args`
* `TaskBase.task_stdout`
* `TaskBase.task_stderr`



## TODO
- [ ] Task graph visualizer
