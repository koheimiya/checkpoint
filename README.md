# taskproc

A lightweight pipeline building/execution/management tool written in pure Python.
Internally, it depends on `DiskCache`, `cloudpickle` `networkx` and `concurrent.futures`.

## Why `taskproc`?
I need a pipeline-handling library that is thin and flexible as much as possible.
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

### Basics

We define a task by class.
```python
from taskproc import Task, Const, Cache

class Choose(Task):
    """ Compute the binomial coefficient. """

    def __init__(self, n: int, k: int):
        # Upstream tasks are prepared here.
        # Al attributes of a task with type `Task` are considered as the upstream tasks.
        if 0 < k < n:
            self.left = Choose(n - 1, k - 1)
            self.right = Choose(n - 1, k)
        elif k == 0 or k == n:
            # We can just pass a value without computation in place of a task.
            self.left = Const(0)
            self.right = Const(1)
        else:
            raise ValueError(f'{(n, k)}')

    def run_task(self) -> int:
        # Here we define the main computation of the task,
        # which is delayed until it is necessary.

        # The return values of the prerequisite tasks are accessible via `.get_result()`:
        return self.left.get_result() + self.right.get_result()

# Construct a concrete task with the class instantiation, which should be inside of `Cache`.
with Cache('./cache'):  # Specifies the cache directory
    task = Choose(6, 3)

# To run the task graph, use the `run_graph()` method.
ans, stats = task.run_graph()  # `ans` should be 6 Choose 3, which is 20.

# It greedily executes all the necessary tasks in the graph as parallel as possible
# and then produces the return value of the task on which we call `run_graph()`, as well as some execution stats.
# The return values of the intermediate tasks are cached on the specified location and reused on the fly whenever possible.
```

### Commandline Interface
`Task` has a utility classmethod to run with commandline arguments, which is useful if all you need is to run a single task that does everything necessary.
For example,
```python
# taskfile.py

class Main(Task):
    def __init__(self):
        self.result = Choose(100, 50)
    
    def run_task(self):
        print(self.result.get_result())


if __name__ == '__main__':
    Main.cli()
```
Use `--help` option for more details.

### Futures and Task Composition

To be more precise, any attributes of a task implementing the `Future` protocol are considered as upstream tasks.
Tasks and constants are futures.
One can pass a future into the initialization of another task to compose the computation.
```python
class MyTask(Task):
    def __init__(self, upstream: Future[int], other_args: Any):
        ...

with Cache('./cache'):
    MyTask(
        upstream=UpstreamTaskProducingInt(),
        ...
    }).run_graph()
```

`FutureList` and `FutureDict` are for aggregating multiple `Future`s into one, allowing us to register multiple upstream tasks at once.
```python
from taskproc import FutureList, FutureDict

class SummarizeScores(Task):

    def __init__(self, task_dict: dict[str, Future[float]]):
        self.score_list = FutureList([ScoringTask(i) for i in range(10)])
        self.score_dict = FutureDict(task_dict)

    def run_task(self) -> float:
        # `.get_result()` evaluates `FutureList[float]` and `FutureDict[str, float]` into
        # `list[float]` and `dict[str, float]`, respectively.
        return sum(self.score_dict.get_result().values()) / len(self.score_dict.get_result())
```

If the output is a sequence or a mapping, one can directly access its element by indexing.
The result is also a `Future`.
```python
class MultiOutputTask(Task):
    def run_task(self) -> dict[str, int]:
        return {'foo': 42, ...}

class DownstreamTask(Task):
    def __init__(self):
        self.dep = MultiOutputTask()['foo']
```

### Input-output Specification

In general, tasks can be initialized with any JSON serializable objects which may or may not contain futures.
Any non-jsonable objects can be also passed, as the output of a task.
```python
SomeTask(1, 'foo', bar={'baz': TaskProducingNonJsonableObj(), 'other': [1, 2, 3]})
```
On the other hand, the output of a task, i.e., the return value of the `.run_task()` method, should be serializable with `cloudpickle`.
The picked object is then compressed with `gzip`.
The compression level can be changed as follows (defaults to 9).
```python
class NoCompressionTask(Task):
    task_compress_level = 0
    ...
```

### Deleting Cache

It is possible to selectively discard cache: 
```python
with Cache('./cache'):
    # After some modificaiton of `Choose(3, 3)`,
    # selectively discard the cache corresponding to the modification.
    Choose(3, 3).clear_task()

    # `ans` is recomputed tracing back to the computation of `Choose(3, 3)`.
    ans, _ = Choose(6, 3).run_graph()
    
    # Delete all the cache associated with `Choose`.
    Choose.clear_all_tasks()            
```

### Data Directories

Use `task.task_directory` to get a fresh path dedicated to each task.
The directory is automatically created and managed along with the cache:
```python
class TrainModel(Task):
    def run_task(self) -> str:
        ...
        model_path = self.task_directory / 'model.bin'
        model.save(model_path)
        return model_path
```


### Prefix Command
Tasks can be run with a prefix command, which is useful when working with a third-party job-scheduling or containerization command such as `jbsub` and `docker`.
```python

class TaskWithJobScheduler(Task):
    task_prefix_command = 'jbsub -wait -queue x86_1h -cores 16+1 -mem 64g'
    ...
```

### Execution Policy Configuration

One can control the task execution with `concurrent.futures.Executor` class:
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class MyTask(Task):
    ...

with Cache('./cache'):
    # Limit the number of parallel workers
    MyTask().run_graph(executor=ProcessPoolExecutor(max_workers=2))
    
    # Thread-based parallelism
    MyTask().run_graph(executor=ThreadPoolExecutor())
```

One can also control the concurrency at a task level:
```python
class TaskUsingGPU(Task):
    task_channel = 'gpu'
    ...

class AnotherTask(Task):
    task_channel = ['gpu', 'memory']
    ...

with Cache('./cache'):
    SomeDownstreamTask().run_graph(rate_limits={TaskUsingGPU.task_name: 1})  # Execute one at a time
```

### Task Channels

Task prefixes and concurrency limits can be also specified via task channels, which is useful for controlling the computation resource of multiple tasks at once.
```python
class TaskUsingGPU(Task):
    task_channel = 'gpu'
    ...

class AnotherTaskUsingGPU(Task):
    task_channel = ['gpu', 'memory']
    ...

with Cache('./cache'):
    # Channel-level concurrency control
    SomeDownstreamTask().run_graph(
        rate_limits={'gpu': 1, 'memory': 2},
        prefixes={'gpu': 'jbsub -wait -queue x86_1h -cores 16+1 -mem 64g'}
    ) 

```


### Built-in properties/methods
Below is the list of the built-in properties/methods of `Task`. Do not override these attributes in the subclass.

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
| `get_result`           | instance | method   | Get the result of the task (fails if the result is missing) |
| `to_json`              | instance | method   | Serialize itself as a JSON dictionary |
| `clear_task`           | instance | method   | Clear the cache of the task instance |
| `clear_all_tasks`      | class    | method   | Clear the cache of the task class |
| `cli`                  | class    | method   | `run_graph` with command line arguments |

## TODO
- Potential bug
    - Current task argument serialization is not ideal since JSON is mapping two different values into the same text representation (e.g., tuple and list). Consider using consistency check `x == json.loads(json.dumps(x))`, or redesign the format.
- Better UX
    - Add the signature of task to the description to `--kwargs` of CLI.
    - Runtime validation of task arguments with type hint.
    - Add option to not cache result (need to address timestamp peeking and value passing).
- Simplify
    - Drop the support of ThreadPoolExecutor.
- Optional
    - Simple task graph visualizer.
    - Pydantic/dataclass support in task arguments (as an incompatible, but better-UX object with TypedDict).
