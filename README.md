# taskproc

A Python library for building/executing/managing task pipelines that is
<!-- ## Why `taskproc`?
`taskproc` is designed to be as thin and flexible as possible.
Specifically,-->
* Minimal yet sufficient for general use.
* Focused on the composability of tasks for building large/complex pipelines effortlessly.
<!-- * Inflexibility of `Luigi`: The definition of the dependencies and the definition of the task computation is tightly coupled at `luigi.Task`s, 
which is super cumbersome if one tries to edit the pipeline structure without changing the computation of each task.
* Unwieldiness of `Airflow`: It requires a message broker backend separately installed and run in background. It is also incompatible with non-pip package manager (such as Poetry).
* Most of the existing libraries tend to build their own ecosystems that unnecessarily forces the user to follow the specific way of handling pipelines.-->
<!-- `taskproc` aims to provide a language construct for defining computation by composition, ideally as simple as Python's built-in sytax of functions, with the support of automatic and configurable parallel execution and cache management.  -->

#### Features
* User defines a long and complex computation by composing shorter and simpler units of work, i.e., tasks.
* `taskproc` automatically executes them in a distributed way, supporting multithreading/multiprocessing and third-party job-controlling commands such as `jbsub` and `docker`. It also creates/reuses/discards a cache per task automatically. 
* Full type-hinting support.

#### Nonfeatures
* Periodic scheduling
* Automatic retry
* External service integration (GCP, AWS, ...)
* Graphical user interface

## Installation

```
pip install taskproc
```
<!--
## Example
See [here](examples/ml_taskfile.py) for a typical usage of `taskproc`.
-->

## Documentation

### Basics

We define a task by class.
```python
from taskproc import Task, Const, Graph

class Choose(Task):
    """ Compute the binomial coefficient. """

    def __init__(self, n: int, k: int):
        # Declaration of the upstream tasks.
        # Any instance of `Task` registered as an attirbute is considered as an upstream task.
        if 0 < k < n:
            self.left = Choose(n - 1, k - 1)
            self.right = Choose(n - 1, k)
        elif k == 0 or k == n:
            # We can also set dummy tasks with their value already computed.
            self.left = Const(0)
            self.right = Const(1)
        else:
            raise ValueError(f'{(n, k)}')

    def run_task(self) -> int:
        # The main computation of the task, which is delayed until necessary.
        # The return values of the prerequisite tasks are accessible via `.get_result()`.
        return self.left.get_result() + self.right.get_result()

# A task pipeline is constructed with instantiation, which should be done inside the `Graph` context.
with Graph('./cache'):  # Specifies the directory to store the cache.
    task = Choose(6, 3)  # Builds a pipeline to compute 6 chooses 3.

# Use the `run_graph()` method to run the pipeline.
ans, stats = task.run_graph()  # `ans` should be 6 chooses 3, which is 20. `stats` is the execution statistics.
```
<!-- # It greedily executes all the necessary tasks in the graph as parallel as possible
# and then produces the return value of the task on which we call `run_graph()`,
# as well as some execution stats. The return values of the intermediate tasks are
# cached on the specified location and reused on the fly whenever possible.-->

### Commandline Interface
`taskproc` has a utility classmethod to run with commandline arguments, which is useful if all you need is to run a single task.
For example, if you have
```python
# taskfile.py
from taskproc import Task, CLIDefaultArguments
# ...

class Main(Task):
    def __init__(self):
        self.result = Choose(100, 50)
    
    def run_task(self):
        print(self.result.get_result())

# Optionally you can configure default CLI arguments.
CLIDefaultArguments(
    # ...
).populate()
```

Then `Main` task can be run with CLI:
```bash
taskproc /path/to/taskfile.py -o /path/to/cache/directory
```
or
```bash
python -m taskproc /path/to/taskfile.py -o /path/to/cache/directory
```
Besides, if you have the entrypoint inside some module, you can run it with
```python
# taskfile.py
...

class Main(Task):
    ...

# Must call the entrypoint explicitly.
if __name__ == '__main__':
    Main.cli()
```
and
```bash
python -m module.path.to.taskfile -o /path/to/cache/directory
```
See also `taskproc /path/to/taskfile.py --help` or `python -m module.path.to.taskfile --help` for more details.


### Futures and Task Composition

To be more precise, any attributes of a task implementing the `Future` protocol are considered as upstream tasks.
For example, `Task`s and `Const`s are `Future`s.
One can pass a future into the initialization of another task to compose the computation.
```python
from taskproc import Future

class DownstreamTask(Task):
    def __init__(self, upstream: Future[int], other_args: Any):
        self.upstream = upstream  # Register upstream task
        ...

class Main(Task):
    def __init__(self):
        self.composed = DownstreamTask(
            upstream=UpstreamTaskProducingInt(),
            ...
        )
```

`FutureList` and `FutureDict` can be used to aggregate multiple futures into one, allowing us to register a batch of upstream futures.
```python
from taskproc import FutureList, FutureDict

class SummarizeScores(Task):
    def __init__(self, task_dict: dict[str, Future[float]]):
        self.score_list = FutureList([ScoringTask(i) for i in range(10)])
        self.score_dict = FutureDict(task_dict)

    def run_task(self) -> float:
        # `.get_result()` evaluates `FutureList[T]` and `FutureDict[K, T]` into
        # `list[T]` and `dict[K, T]`, respectively.
        return sum(self.score_dict.get_result().values()) / len(self.score_dict.get_result())
```

If a future is wrapping a sequence or a mapping, one can directly access its element with the standard indexing operation.
The result is also a `Future`.
```python
class MultiOutputTask(Task):
    def run_task(self) -> dict[str, int]:
        return {'foo': 42, ...}

class DownstreamTask(Task):
    def __init__(self):
        self.dep = MultiOutputTask()['foo']  # type of Future[int]
```


### Input and Output Specifications

In general, tasks can be initialized with any JSON serializable objects which may or may not contain futures.
Any non-jsonable objects can be also passed, as the output of a task.
```python
SomeTask(1, 'foo', bar={'baz': TaskProducingNonJsonableObj(), 'other': [1, 2, 3]})
```
On the other hand, the output of a task, i.e., the return value of the `.run_task()` method, should be serializable with `cloudpickle`.


### Data Directories

Use `task.task_directory` to get a fresh path dedicated to each task.
The directory is automatically created and managed along with the cache.
```python
class TrainModel(Task):
    def run_task(self) -> str:
        ...
        model_path = self.task_directory / 'model.bin'
        model.save(model_path)
        return model_path
```

### Task Label for Computational Resource Control

Each task class can be tagged with multiple labels.
The task labels are useful to configure prefix commands and concurrency limits for controlling of computational resources.
```python
class TaskUsingGPU(Task):
    task_label = 'gpu'
    ...

class AnotherTaskUsingGPU(Task):
    task_label = ['gpu', 'memory']
    ...

with Graph('./cache'):
    # Label-wise prefix/concurrency control
    SomeDownstreamTask().run_graph(
        # The number of tasks labeled with "gpu" running simultaneously is at most 2 (resp. "memory" is at most 1).
        rate_limits={
            'gpu': 2,
            'memory': 1,
            TaskUsingGPU.task_name: 5,  # Each task is also labeld with `cls.task_name` by default.
            },
        prefixes={
            # Task labeled with "gpu" is run with the job-dispatching command "jbsub ...".
            # Left labels prevail in the prefix collision.
            'gpu': 'jbsub -wait -queue x86_1h -cores 16+1 -mem 64g'  
            }
    ) 
```


### Advanced Topics

#### Execution Policy Configuration

One can control the policy of parallelism with `concurrent.futures.Executor` classes.
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class MyTask(Task):
    ...

with Graph('./cache'):
    # Limit the number of parallel workers
    MyTask().run_graph(executor=ProcessPoolExecutor(max_workers=2))
    
    # Thread-based parallelism
    MyTask().run_graph(executor=ThreadPoolExecutor())
```

#### Selective Cache Deletion

It is possible to selectively discard cache: 
```python
with Graph('./cache'):
    # Selectively discard the cache of a specific task.
    Choose(3, 3).clear_task()

    # `ans` is recomputed tracing back to the computation of `Choose(3, 3)`.
    ans, _ = Choose(6, 3).run_graph()
    
    # Delete all the cache associated with `Choose`.
    Choose.clear_all_tasks()            
```
One can also manage caches directly from the disk location, i.e., `./cache` in the above.

#### Cache Compression
The task output is compressed with `gzip`.
The compression level can be changed as follows (defaults to 9), trading the space efficiency with the time efficiency.
```python
class NoCompressionTask(Task):
    task_compress_level = 0
    ...
```

#### Built-in properties/methods
Below is the list of the built-in attributes/properties/methods of `Task`. Do not override these attributes in the subclass.

| Name | Owner | Type | Description |
|--|--|--|--|
| `run_task`            | instance  | method    | Run the task |
| `task_name`           | class     | property  | String id of the task class |
| `task_directory`      | instance  | property  | Path to the data directory of the task |
| `run_graph`           | instance  | method    | Run the task after necessary upstream tasks and save the results in the cache |
| `cli`                 | class     | method    | `run_graph` with command line arguments |
| `clear_task`          | instance  | method    | Clear the cache of the task instance |
| `clear_all_tasks`     | class     | method    | Clear the cache of the task class |
| `get_task_config`     | class     | method    | Get task config from the current graph |
| `task_worker`         | instance  | attribute | Task worker of instance |
| `task_config`         | instance  | attribute | Task config of instance |
| `task_compress_level` | instance  | attribute | Compression level of instance |
| `task_label`          | instance  | attribute | Label of instance |
| `get_result`          | instance  | method    | Get the result of the task (fails if the result is missing) |
| `to_json`             | instance  | method    | Serialize itself as a JSON dictionary |
| `get_workers`         | instance  | method    | Get the dictionary of the workers |


#### Browsing caches
Show the whole task dependency tree:
```bash
tree -l /<path_to_cache_directory>/<task_name>/results/<root_task_id>
```
Show finished tasks only:
```bash
tree -l -P result.pkl.gz --prune /<path_to_cache_directory>/<task_name>/results/<root_task_id>
```
Show finished tasks + running tasks:
```bash
tree -l -P *.txt --prune /<path_to_cache_directory>/<task_name>/results/<root_task_id>
```


## TODO
- Known issue
    - Current task argument serialization is not ideal since JSON is mapping two different values into the same text representation (e.g., tuple and list). Consider using consistency check `x == json.loads(json.dumps(x))`, or redesign the format.

- Feature enhancement
    - Task-state tracker.
    - Simple task graph visualizer.
    - Pydantic/dataclass support in task arguments (as an incompatible, but better-UX object with TypedDict).
    - Dynamic prefix generation with prefix template (e.g., for specifying the log locations).
