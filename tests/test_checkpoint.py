from typing import Any
import pytest
from checkpoint import infer_task_type, Task, Req, Requires, Const, RequiresDict, DataPath


@infer_task_type
class Choose(Task):
    prev1: Requires[int] = Req()
    prev2: Requires[int] = Req()

    def init(self, n: int, k: int):
        if 0 < k < n:
            self.prev1 = Choose(n - 1, k - 1)
            self.prev2 = Choose(n - 1, k)
        else:
            self.prev1 = Const(0)
            self.prev2 = Const(1)

    def main(self) -> int:
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
    ans, stats = Choose(6, 3).run_task_with_stats(rate_limits={Choose.task_config.queue: 2})
    assert ans == 20
    assert sum(stats['stats'].values()) == 15

    """ 0 caches: """
    ans, stats = Choose(6, 3).run_task_with_stats()
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
    ans, stats = Choose(6, 3).run_task_with_stats()
    assert ans == 20
    assert sum(stats['stats'].values()) == 4


@infer_task_type
class TaskA(Task):
    def init(self): ...

    def main(self) -> str:
        return 'hello'


@infer_task_type
class TaskB(Task, queue='myqueue'):
    def init(self): ...
    
    def main(self) -> str:
        return 'world'


@infer_task_type
class TaskC(Task, compress_level=-1):
    a: Requires[str] = Req()
    b: Requires[str] = Req()

    def init(self):
        self.a = TaskA()
        self.b = TaskB()
    
    def main(self) -> str:
        return f'{self.a}, {self.b}'


def test_multiple_tasks():
    TaskA.clear_all_tasks()
    TaskB.clear_all_tasks()
    TaskC.clear_all_tasks()
    assert TaskC().run_task() == 'hello, world'
    assert TaskB.task_config.queue == 'myqueue'
    assert TaskC.task_config.db.compress_level == -1


@infer_task_type
class TaskRaise(Task):
    def init(self): ...
    def main(self):
        raise ValueError(42)


def test_raise():
    with pytest.raises(ValueError):
        TaskRaise().run_task()


@infer_task_type
class CreateFile(Task):
    outpath = DataPath('test.txt')

    def init(self, content: str):
        self.content = content

    def main(self) -> str:
        with open(self.outpath, 'w') as f:
            f.write(self.content)
        return str(self.outpath)


@infer_task_type
class GreetWithFile(Task):
    filepath: Requires[str] = Req()

    def init(self, name: str):
        self.filepath = CreateFile(f'Hello, {name}!')

    def main(self) -> str:
        with open(self.filepath, 'r') as f:
            return f.read()


def test_requires_directory():
    CreateFile.clear_all_tasks()
    GreetWithFile.clear_all_tasks()
    taskdir_world = CreateFile('Hello, world!').task_worker._directory_uninit
    taskdir_me = CreateFile('Hello, me!').task_worker._directory_uninit
    task_factory_dir = CreateFile.task_config.data_directory

    def check_output(name: str):
        assert GreetWithFile(name).run_task() == f'Hello, {name}!'

    assert not taskdir_world.exists()
    assert not taskdir_me.exists()
    assert not any(task_factory_dir.iterdir())
    check_output('world')
    check_output('me')
    assert taskdir_world.exists()
    assert taskdir_me.exists()
    assert any(task_factory_dir.iterdir())

    # Directories persist
    GreetWithFile.clear_all_tasks()
    check_output('world')

    # Specific task directory can be deleted
    CreateFile('Hello, world!').clear_task()
    assert not taskdir_world.exists()       # task directory deleted
    assert taskdir_me.exists()              # other task directories are not deleted
    assert any(task_factory_dir.iterdir())  # whole task directory is not deleted
    check_output('world')                   # file recreated

    # Task directory can be deleted at all
    CreateFile.clear_all_tasks()
    assert not taskdir_world.exists()           # task directory deleted
    assert not taskdir_me.exists()              # other task directories are also deleted
    assert not any(task_factory_dir.iterdir())  # whole task directory is deleted
    check_output('world')                       # file recreated


@infer_task_type
class CountElem(Task):
    def init(self, x: list | dict):
        self.x = x

    def main(self) -> int:
        return len(self.x)


@infer_task_type
class SummarizeParam(Task):
    d_counts: RequiresDict[str, int] = Req()

    def init(self, **params: Any):
        self.a_params = params
        self.a_container_keys = [k for k in params if isinstance(params[k], (list, dict))]
        self.d_counts = {k: CountElem(params[k]) for k in self.a_container_keys}

    def main(self) -> dict[str, int | None]:
        out: dict[str, int | None] = dict(self.d_counts)
        out.update({k: None for k in self.a_params if k not in self.a_container_keys})
        return out


def test_json_param():
    res = SummarizeParam(x=[1, 2], y=dict(zip(range(3), 'abc')), z=42).run_task()
    assert res == {'x': 2, 'y': 3, 'z': None}
