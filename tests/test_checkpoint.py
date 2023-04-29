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
def create_fresh_directory():
    @requires_directory
    def run_task(path) -> str:
        return str(path)
    return run_task


def test_requires_directory():
    create_fresh_directory().run()
