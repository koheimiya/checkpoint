from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pdb import run
from typing import Any
import pytest
from checkpoint import requires_directory, task, requires


@task(max_concurrency=1)
def choose(n: int, k: int):
    if 0 < k < n:
        @requires([choose(n - 1, k - 1), choose(n - 1, k)])
        def __(prev_two: list[int]):
            return sum(prev_two)

    elif k == 0 or k == n:
        def __() -> int:
            return 1

    else:
        raise ValueError(f'{(n, k)}')
    return __


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
    choose.clear()
    ans, info = choose(6, 3).run_with_info()
    assert ans == 20
    assert sum(info['stats'].values()) == 15

    """ 0 caches: """
    ans, info = choose(6, 3).run_with_info()
    assert ans == 20
    assert sum(info['stats'].values()) == 0

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
    choose(3, 3).clear()
    ans, info = choose(6, 3).run_with_info()
    assert ans == 20
    assert sum(info['stats'].values()) == 4

@task
def task_a():
    def __():
        return None
    return __


@task(compress_level=6)
def task_b():
    def __():
        return None
    return __


@task(compress_level=6)
def task_c():
    @requires(task_a())
    @requires(task_b())
    def __(a: None, b: None):
        return 42
    return __


def test_multiple_tasks():
    task_a.clear()
    task_b.clear()
    task_c.clear()
    assert task_c().run() == 42


@task(compress_level=6)
def task_raise():
    @requires(task_a())
    @requires(task_b())
    def __(a: None, b: None):
        raise ValueError(42)
    return __


def test_raise():
    with pytest.raises(ValueError):
        task_raise().run()


@task
def create_file(content: str):
    @requires_directory
    def __(path: Path) -> str:
        outpath = path / 'test.txt'
        with open(outpath, 'w') as f:
            f.write(content)
        return str(outpath)
    return __

@task
def greet_with_file(name):
    @requires(create_file(f'Hello, {name}!'))
    def __(path: str) -> str:
        with open(path, 'r') as f:
            return f.read()
    return __


def test_requires_directory():
    create_file.clear()
    greet_with_file.clear()
    taskdir_world = create_file('Hello, world!').directory
    taskdir_me = create_file('Hello, me!').directory
    task_factory_dir = create_file.db.data_directory

    def check_output(name: str):
        assert greet_with_file(name).run() == f'Hello, {name}!'

    assert not taskdir_world.exists()
    assert not taskdir_me.exists()
    assert not any(task_factory_dir.iterdir())
    check_output('world')
    check_output('me')
    assert taskdir_world.exists()
    assert taskdir_me.exists()
    assert any(task_factory_dir.iterdir())

    # Directories persist
    greet_with_file.clear()
    check_output('world')

    # Specific task directory can be deleted
    create_file('Hello, world!').clear()
    assert not taskdir_world.exists()       # task directory deleted
    assert taskdir_me.exists()              # other task directories are not deleted
    assert any(task_factory_dir.iterdir())  # whole task directory is not deleted
    check_output('world')                   # file recreated

    # Task directory can be deleted at all
    create_file.clear()
    assert not taskdir_world.exists()           # task directory deleted
    assert not taskdir_me.exists()              # other task directories are also deleted
    assert not any(task_factory_dir.iterdir())  # whole task directory is deleted
    check_output('world')                       # file recreated


@task
def count_elem(x: list | dict):
    def __() -> int:
        return len(x)
    return __


@task
def summarize_param(**params: Any):
    container_keys = [k for k in params if isinstance(params[k], (list, dict))]

    @requires({k: count_elem(params[k]) for k in container_keys})
    def __(stats: dict[str, int]) -> dict[str, int | None]:
        out: dict[str, int | None] = dict(stats)
        out.update({k: None for k in params if k not in container_keys})
        return out
    return __


def test_json_param():
    res = summarize_param(x=[1, 2], y=dict(zip(range(3), 'abc')), z=42).run()
    assert res == {'x': 2, 'y': 3, 'z': None}
