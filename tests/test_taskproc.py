from concurrent.futures import ThreadPoolExecutor
from typing import Any
import pytest
from taskproc import infer_task_type, TaskBase, Req, Requires, Const, RequiresDict


@infer_task_type
class Choose(TaskBase):
    prev1: Requires[int] = Req()
    prev2: Requires[int] = Req()

    def build_task(self, n: int, k: int):
        if 0 < k < n:
            self.prev1 = Choose(n - 1, k - 1)
            self.prev2 = Choose(n - 1, k)
        else:
            self.prev1 = Const(0)
            self.prev2 = Const(1)

    def run_task(self) -> int:
        return self.prev1 + self.prev2


def test_graph():
    """ 15 caches:
     0123
    0.
    1xx
    2xxx
    3xxxx
    4.xxx
    5..xx
    6...x
    """
    Choose.clear_all_tasks()
    ans, stats = Choose(6, 3).run_graph_with_stats(rate_limits={Choose.task_name: 2})
    assert ans == 20
    assert sum(stats['stats'].values()) == 15

    """ 0 caches: """
    ans, stats = Choose(6, 3).run_graph_with_stats()
    assert ans == 20
    assert sum(stats['stats'].values()) == 0

    """ 4 caches:
     0123
    0.
    1..
    2...
    3...x
    4...x
    5...x
    6...x
    """
    Choose(3, 3).clear_task()
    ans, stats = Choose(6, 3).run_graph_with_stats()
    assert ans == 20
    assert sum(stats['stats'].values()) == 4


@infer_task_type
class TaskA(TaskBase, channel=['<mychan>', '<another_chan>']):
    def build_task(self): ...

    def run_task(self) -> str:
        return 'hello'


@infer_task_type
class TaskB(TaskBase, channel='<mychan>'):
    def build_task(self): ...
    
    def run_task(self) -> str:
        return 'world'


@infer_task_type
class TaskC(TaskBase, compress_level=-1):
    a: Requires[str] = Req()
    b: Requires[str] = Req()

    def build_task(self):
        self.a = TaskA()
        self.b = TaskB()
    
    def run_task(self) -> str:
        return f'{self.a}, {self.b}'


def test_multiple_tasks():
    TaskA.clear_all_tasks()
    TaskB.clear_all_tasks()
    TaskC.clear_all_tasks()
    assert TaskC().run_graph(rate_limits={'<mychan>': 1}) == 'hello, world'
    assert TaskB._task_config.channels == (TaskB._task_config.name, '<mychan>')
    assert TaskC._task_config.db.compress_level == -1


@infer_task_type
class TaskRaise(TaskBase):
    def build_task(self): ...
    def run_task(self):
        raise ValueError(42)


def test_raise():
    with pytest.raises(ValueError):
        TaskRaise().run_graph()


@infer_task_type
class CreateFile(TaskBase):

    def build_task(self, content: str):
        self.content = content

    def run_task(self) -> str:
        outpath = self.task_directory / 'test.txt'
        with open(outpath, 'w') as f:
            f.write(self.content)
        return str(outpath)


@infer_task_type
class GreetWithFile(TaskBase):
    filepath: Requires[str] = Req()

    def build_task(self, name: str):
        self.filepath = CreateFile(f'Hello, {name}!')

    def run_task(self) -> str:
        with open(self.filepath, 'r') as f:
            return f.read()


def test_requires_directory():
    CreateFile.clear_all_tasks()
    GreetWithFile.clear_all_tasks()
    taskdir_world = CreateFile('Hello, world!')._task_worker._data_directory_uninit
    taskdir_me = CreateFile('Hello, me!')._task_worker._data_directory_uninit

    def check_output(name: str):
        assert GreetWithFile(name).run_graph() == f'Hello, {name}!'

    assert not taskdir_world.exists()
    assert not taskdir_me.exists()
    check_output('world')
    check_output('me')
    assert taskdir_world.exists()
    assert taskdir_me.exists()

    # Directories persist
    GreetWithFile.clear_all_tasks()
    check_output('world')

    # Specific task directory can be deleted
    CreateFile('Hello, world!').clear_task()
    assert not taskdir_world.exists()       # task directory deleted
    assert taskdir_me.exists()              # other task directories are not deleted
    check_output('world')                   # file recreated

    # Task directory can be deleted at all
    CreateFile.clear_all_tasks()
    assert not taskdir_world.exists()           # task directory deleted
    assert not taskdir_me.exists()              # other task directories are also deleted
    check_output('world')                       # file recreated


@infer_task_type
class CountElem(TaskBase):
    def build_task(self, x: list | dict):
        self.x = x

    def run_task(self) -> int:
        return len(self.x)


@infer_task_type
class SummarizeParam(TaskBase):
    d_counts: RequiresDict[str, int]

    def build_task(self, **params: Any):
        self.a_params = params
        self.a_container_keys = [k for k in params if isinstance(params[k], (list, dict))]
        self.d_counts = {k: CountElem(params[k]) for k in self.a_container_keys}

    def run_task(self) -> dict[str, int | None]:
        out: dict[str, int | None] = dict(self.d_counts)
        out.update({k: None for k in self.a_params if k not in self.a_container_keys})
        return out


def test_json_param():
    res = SummarizeParam(x=[1, 2], y=dict(zip(range(3), 'abc')), z=42).run_graph()
    assert res == {'x': 2, 'y': 3, 'z': None}


@infer_task_type
class MultiResultTask(TaskBase):
    def build_task(self) -> None:
        pass

    def run_task(self) -> dict[str, str]:
        return {'hello': 'world'}


@infer_task_type
class DownstreamTask(TaskBase):
    up: Requires[str]

    def build_task(self) -> None:
        self.up = MultiResultTask()['hello']

    def run_task(self) -> str:
        return self.up


def test_mapping():
    MultiResultTask.clear_all_tasks()
    DownstreamTask.clear_all_tasks()
    assert DownstreamTask().run_graph() == 'world'


@infer_task_type
class PrefixedJob(TaskBase, prefix_command='bash tests/run_with_hello.bash'):
    def run_task(self) -> None:
        print('world')
        return
    ...


def test_prefix_command(capsys):
    PrefixedJob.clear_all_tasks()
    task = PrefixedJob()
    task.run_graph(executor=ThreadPoolExecutor(max_workers=1))
    captured = capsys.readouterr()
    assert captured.out == ''
    assert captured.err == ''

    assert open(task.task_stdout, 'r').read() == 'hello\nworld\n'
