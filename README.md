# taskproc

A lightweight pipeline building/execution/management tool written in pure Python.
Internally, it depends on `DiskCache`, `cloudpickle` `networkx` and `concurrent.futures`.

## Why `taskproc`?
I needed a pipeline-handling library that is thin and flexible as much as possible.
* `Luigi` is not flexible enough: The definition of the dependencies and the definition of the task computation is tightly coupled at `luigi.Task`s, 
which is super cumbersome if one tries to edit the pipeline structure without changing the computation of each task.
* `Airflow` is too big and clumsy: It requires a message broker backend separately installed and run in background. It is also incompatible with non-pip package manager (such as Poetry).
* Most of the existing libraries tend to build their own ecosystems that unnecessarily forces the user to follow the specific way of handling pipelines.

`taskproc` aims to provide a language construct for defining computation by composition, ideally as simple as Python's built-in sytax of functions, with the support of automatic and configurable parallel execution and cache management.  

#### Features
* Decomposing long and complex computation into tasks, i.e., smaller units of work with dependencies.
* Executing them in a distributed way, supporting multithreading/multiprocessing and local container/cluster-based dispatching.
* Automatically creating/discarding/reusing caches per task. 

#### Nonfeatures
* Periodic scheduling
* Automatic retry
* External service integration (GCP, AWS, ...)
* Graphical user interface

## Installation

```
pip install taskproc
```

## Example
See [here](examples/ml_taskfile.py) for a typical usage of `taskproc`.

## Documentation

### Defining task

Pipeline is a directed acyclic graph (DAG) of tasks with a single sink node (i.e., final task), where task is a unit of work represented with a class.
Each task and its upstream dependencies are specified with a class definition like so:
```python
from taskproc import TaskBase, Requires, Const

class Choose(TaskBase):
    """ Compute the binomial coefficient. """
    # Inside a task, we first declare the values that must be computed in upstream.
    # In this example, `Choose(n, k)` depends on `Choose(n - 1, k - 1)` and `Choose(n - 1, k)`,
    # so it requires two `int` values.
    prev1: Requires[int]
    prev2: Requires[int]

    def __init__(self, n: int, k: int):
        # The upstream tasks and the other instance attributes are prepared here.
        # It thus recursively defines all the tasks we need to run this task,
        # i.e., the entire upstream pipeline.

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

# To run the task as well as the upstreams, use the `run_graph()` method.
ans = Choose(6, 3).run_graph()  # `ans` should be 6 Choose 3, which is 20.

# It greedily executes all the necessary tasks as parallel as possible
# and then spits out the return value of the task on which we call `run_graph()`.
# The return values of the intermediate tasks are cached at
# `{$TP_CACHE_DIR:-./.cache}/taskproc/{module_name}.{task_name}/results/...`
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
# equivalent to `rm -r {$TP_CACHE_DIR:-./.cache}/taskproc/{module_name}.Choose`.
Choose.clear_all_tasks()            
```

### Task IO

The arguments of the `__init__` method can be anything JSON serializable + `Task`s:
```python
class MyTask(TaskBase):
    def __init__(self, param1, param2):
        ...

MyTask(
    param1={
        'upstream_task0': UpstreamTask(),
        'other_params': [1, 2],
        ...
    },
    param2={ ... }
}).run_graph()
```

List/dict of upstream tasks can be registered with `RequiresList` and `RequiresDict`:
```python
from taskproc import RequiresList, RequiresDict

class SummarizeScores(TaskBase):
    score_list: RequiresList[float]
    score_dict: RequiresDict[str, float]

    def __init__(self, task_dict: dict[str, Task[float]]):
        self.score_list = [MyScore(i) for i in range(10)]
        self.score_dict = task_dict

    def run_task(self) -> float:
        # At runtime `self.score_list` and `self.score_dict` are evaluated as
        # `list[float]` and `dict[str, float]`, respectively.
        return sum(self.score_dict.values()) / len(self.score_dict)
```

The output of the `run_task` method should be serializable with `cloudpickle`,
which is then compressed with `gzip`.
The compression level can be changed as follows (defaults to 9).
```python
class NoCompressionTask(TaskBase, compress_level=0):
    ...
```

If the output is a dictionary, one can directly access its element:
```python
class MultiOutputTask(TaskBase):
    def run_task(self) -> dict[str, int]:
        return {'foo': 42, ...}

class DownstreamTask(TaskBase):
    dep: Requires[int]

    def __init__(self):
        self.dep = MultiOutputTask()['foo']
```

### Data directories

Use `task.task_directory` to get a fresh path dedicated to each task.
The directory is automatically created and managed along with the task cache:
The contents of the directory are cleared at each task call and persist until the task is cleared.
```python
class TrainModel(TaskBase):
    def run_task(self) -> str:
        ...
        model_path = self.task_directory / 'model.bin'
        model.save(model_path)
        return model_path
```


### Job scheduling and prefixes
Tasks can be run with job schedulers using `prefix_command`, which will be inserted just before each task call.
```python

class TaskWithJobScheduler(TaskBase, prefix_command='jbsub -interactive -tty -queue x86_1h -cores 16+1 -mem 64g'):
    ...
```


### Interactive tasks
Interactive task is beneficial for debugging (e.g., with breakpoints) but harmful for parallel computing.
In `taskproc`, all the tasks are executed detached by default.
One can make them interactive by explicitly specifying like so:
```python

class InteractiveTask(TaskBase, interactive=True):
    ...
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
We can use taskproc from commandline like `taskproc path/to/taskfile.py`, where `taskfile.py` defines the `Main` task as follows:
```python
# taskfile.py

class Main(TaskBase):
    ...
```
The command runs the `Main` task and stores the cache at `{$TP_CACHE_DIR:-./.cache}/taskproc/...`.
Please refer to `taskproc --help` for more info.


### Built-in properties
Below is the list of the built-in properties/methods of `TaskBase`. Do not override these attributes in the subclass.

| Name | Owner | Type | Description |
|--|--|--|--|
| `task_name`            | class    | property | String id of the task class |
| `task_id`              | instance | property | Integer id of the task, unique within the same task class  |
| `task_args`            | instance | property | The arguments of the task in JSON |
| `task_directory`       | instance | property | Path to the data directory of the task |
| `task_stdout`          | instance | property | Path to the task's stdout |
| `task_stderr`          | instance | property | Path to the task's stderr |
| `run_task`             | instance | method   | Run the task |
| `run_graph`            | instance | method   | Run the task after necessary upstream tasks and save the results in the cache |
| `run_graph_with_stats` | instance | method   | `run_graph` with additional statistics |
| `get_task_result`      | instance | method   | Directly get the result of the task (fails if the cache is missing) |
| `clear_task`           | instance | method   | Clear the cache of the task instance |
| `clear_all_tasks`      | class    | method   | Clear the cache of the task class |

## TODO
- [ ] Queue-wise prefix allocation
- [ ] Add local executor
- [ ] Task graph visualizer
