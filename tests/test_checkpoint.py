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
        def run_task(prev_two: list[int]):
            return sum(prev_two)

    elif k == 0 or k == n:
        def run_task() -> int:
            return 1

    else:
        raise ValueError(f'{(n, k)}')
    return run_task


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
    def run_task():
        return None
    return run_task


@task(compress_level=6)
def task_b():
    def run_task():
        return None
    return run_task


@task(compress_level=6)
def task_c():
    @requires(task_a())
    @requires(task_b())
    def run_task(a: None, b: None):
        return 42
    return run_task


def test_multiple_tasks():
    task_a.clear()
    task_b.clear()
    task_c.clear()
    assert task_c().run() == 42


@task(compress_level=6)
def task_raise():
    @requires(task_a())
    @requires(task_b())
    def run_task(a: None, b: None):
        raise ValueError(42)
    return run_task


def test_raise():
    with pytest.raises(ValueError):
        task_raise().run()


@task
def create_file():
    @requires_directory
    def run_task(path: Path) -> str:
        outpath = path / 'test.txt'
        with open(outpath, 'w') as f:
            f.write('hello')
        return str(outpath)
    return run_task

@task
def peek_content():
    @requires(create_file())
    def run_task(path: str) -> str:
        with open(path, 'r') as f:
            return f.read()
    return run_task


def test_requires_directory():
    create_file.clear()
    peek_content.clear()
    taskdir = peek_content().directory
    task_factory_dir = peek_content.db.data_directory
    assert not taskdir.exists()  # task directory not created yet

    assert peek_content().run() == 'hello'
    peek_content.clear()
    assert peek_content().run() == 'hello'  # files persist

    create_file.clear()
    assert not taskdir.exists()                 # task directory deleted
    assert not any(task_factory_dir.iterdir())  # task factory directory is empty
    assert peek_content().run() == 'hello'      # files recreated

    create_file().clear()
    assert not taskdir.exists()             # task directory deleted
    assert peek_content().run() == 'hello'  # files recreated


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
