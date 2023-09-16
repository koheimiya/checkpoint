from concurrent.futures import ThreadPoolExecutor
from typing import Any
import pytest
from taskproc import Task, Future, Const, Cache, FutureList, FutureDict
import time
from taskproc.graph import FailedTaskError


class Choose(Task):
    def __init__(self, n: int, k: int):
        if 0 < k < n:
            self.prev1 = Choose(n - 1, k - 1)
            self.prev2 = Choose(n - 1, k)
        else:
            self.prev1 = Const(0)
            self.prev2 = Const(1)

    def run_task(self) -> int:
        return self.prev1.get_result() + self.prev2.get_result()


@Cache('./.cache/tests')
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
    ans, stats = Choose(6, 3).run_graph()
    assert ans == 20
    assert sum(stats['stats'].values()) == 15

    """ 0 caches: """
    ans, stats = Choose(6, 3).run_graph()
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
    ans, stats = Choose(6, 3).run_graph()
    assert ans == 20
    assert sum(stats['stats'].values()) == 4


class TaskA(Task):
    task_channel = ['<mychan>', '<another_chan>']
    def __init__(self): ...

    def run_task(self) -> str:
        return 'hello'


class TaskB(Task):
    task_channel = '<mychan>'
    def __init__(self): ...
    
    def run_task(self) -> str:
        return 'world'


class TaskC(Task):
    task_compress_level = 0

    def __init__(self):
        self.a = TaskA()
        self.b = TaskB()
    
    def run_task(self) -> str:
        return f'{self.a.get_result()}, {self.b.get_result()}'


@Cache('./.cache/tests')
def test_multiple_tasks():
    TaskA.clear_all_tasks()
    TaskB.clear_all_tasks()
    TaskC.clear_all_tasks()
    main = TaskC()
    assert main.run_graph(rate_limits={'<mychan>': 1})[0] == 'hello, world'
    assert TaskB()._task_worker.channels == (TaskB.task_config.name, '<mychan>')
    assert main.task_compress_level == 0


class TaskRaise(Task):
    def __init__(self): ...
    def run_task(self):
        raise ValueError(42)


@Cache('./.cache/tests')
def test_raise():
    with pytest.raises(FailedTaskError):
        TaskRaise().run_graph()


class CreateFile(Task):
    def __init__(self, content: str):
        self.content = content

    def run_task(self) -> str:
        outpath = self.task_directory / 'test.txt'
        with open(outpath, 'w') as f:
            f.write(self.content)
        return str(outpath)


class GreetWithFile(Task):
    def __init__(self, name: str):
        self.filepath = CreateFile(f'Hello, {name}!')

    def run_task(self) -> str:
        with open(self.filepath.get_result(), 'r') as f:
            return f.read()


@Cache('./.cache/tests')
def test_requires_directory():
    CreateFile.clear_all_tasks()
    GreetWithFile.clear_all_tasks()
    taskdir_world = CreateFile('Hello, world!').task_directory
    taskdir_me = CreateFile('Hello, me!').task_directory

    def check_output(name: str):
        assert GreetWithFile(name).run_graph()[0] == f'Hello, {name}!'

    assert not list(taskdir_world.iterdir())
    assert not list(taskdir_me.iterdir())
    check_output('world')
    check_output('me')
    assert list(taskdir_world.iterdir())
    assert list(taskdir_me.iterdir())

    # Directories persist
    GreetWithFile.clear_all_tasks()
    check_output('world')

    # Specific task directory can be deleted
    CreateFile('Hello, world!').clear_task()
    assert not list(taskdir_world.iterdir())  # task directory deleted
    assert list(taskdir_me.iterdir())         # other task directories are not deleted
    check_output('world')                     # file recreated

    # Task directory can be deleted at all
    CreateFile.clear_all_tasks()
    assert not taskdir_world.exists()    # task directory deleted
    assert not taskdir_me.exists()       # other task directories are also deleted
    check_output('world')                # file recreated


class CountElem(Task):
    def __init__(self, x: list | dict):
        self.x = x

    def run_task(self):
        return len(self.x)


class SummarizeParam(Task):
    def __init__(self, **params: Any):
        self.a_params = params
        self.a_container_keys = [k for k in params if isinstance(params[k], (list, dict))]
        self.d_counts = FutureDict({k: CountElem(params[k]) for k in self.a_container_keys})

    def run_task(self) -> dict[str, int | None]:
        out: dict[str, int | None] = dict(self.d_counts.get_result())
        out.update({k: None for k in self.a_params if k not in self.a_container_keys})
        return out


@Cache('./.cache/tests')
def test_json_param():
    res, _ = SummarizeParam(x=[1, 2], y=dict(zip(range(3), 'abc')), z=42).run_graph()
    assert res == {'x': 2, 'y': 3, 'z': None}


class MultiResultTask(Task):
    def __init__(self) -> None:
        pass

    def run_task(self):
        return {'hello': ['world', '42']}


class DownstreamTask(Task):
    def __init__(self) -> None:
        self.up = MultiResultTask()['hello'][1]

    def run_task(self) -> str:
        return self.up.get_result()


@Cache('./.cache/tests')
def test_mapping():
    MultiResultTask.clear_all_tasks()
    DownstreamTask.clear_all_tasks()
    assert DownstreamTask().run_graph()[0] == '42'


class PrefixedJob(Task):
    task_prefix_command = 'bash tests/run_with_hello.bash'
    task_channel = 'mychan'
    def run_task(self) -> None:
        print('world')
        return


@Cache('./.cache/tests')
def test_prefix_command(capsys):
    PrefixedJob.clear_all_tasks()
    task = PrefixedJob()
    task.run_graph(executor=ThreadPoolExecutor(max_workers=1))
    captured = capsys.readouterr()
    assert captured.out == ''
    assert captured.err == ''

    assert open(task.task_stdout, 'r').read() == '=== caller log ===\nhello\n=== callee log ===\nworld\n'


@Cache('./.cache/tests')
def test_prefix_command2(capsys):
    PrefixedJob.clear_all_tasks()
    task = PrefixedJob()
    task.run_graph(executor=ThreadPoolExecutor(max_workers=1), prefixes={'mychan': ''})
    captured = capsys.readouterr()
    assert captured.out == ''
    assert captured.err == ''

    assert open(task.task_stdout, 'r').read() == '=== caller log ===\n=== callee log ===\nworld\n'


class SleepTask(Task):
    def __init__(self, *prevs: Future[float]):
        self.prevs = FutureList(prevs)

    def run_task(self):
        t = .5
        time.sleep(t)
        return t + max(self.prevs.get_result(), default=0)


@Cache('./.cache/tests')
def test_sleep_task():
    SleepTask.clear_all_tasks()
    task1 = SleepTask()
    task2 = SleepTask()
    task3 = SleepTask(task1)
    task4 = SleepTask(task2)
    task5 = SleepTask(task3, task4)
    start = time.perf_counter()
    task5.run_graph()
    elapsed = time.perf_counter() - start
    assert elapsed < 2


class InteractiveJob(Task):
    def run_task(self) -> None:
        print('world')
        return


@Cache('./.cache/tests')
def test_interactive(capsys):
    InteractiveJob.clear_all_tasks()
    task = InteractiveJob()
    task.run_graph(executor=ThreadPoolExecutor(max_workers=1), force_interactive=True)
    captured = capsys.readouterr()
    assert captured.out == 'world\n'
    assert captured.err == ''

    assert not task.task_stdout.exists()
    assert not task.task_stderr.exists()


def test_context():
    with Cache('./.cache/tests/1'):
        Choose(3, 2).run_graph()

    with Cache('./.cache/tests/2'):
        Choose.clear_all_tasks()

    with Cache('./.cache/tests/1'):
        assert Choose(3, 2)._task_worker.peek_timestamp() is not None

    with pytest.raises(RuntimeError):
         Choose(3, 2)

    with Cache('./.cache/tests/1'):
        Choose.clear_all_tasks()
