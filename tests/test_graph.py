from checkpoint.graph import task, requires


@task(max_concurrency=1)
def choose(n: int, k: int):

    if 0 < k < n:
        @requires([choose(n - 1, k - 1), choose(n - 1, k)])
        def run(prev_two: list[int]):
            return sum(prev_two)

    elif k == 0 or k == n:
        def run() -> int:
            return 1

    else:
        raise ValueError(f'{(n, k)}')
    return run


def test_graph():
    choose.clear()
    ans, info = choose(6, 3).run_with_info()
    assert ans == 20
    assert sum(info['stats'].values()) == 15

    ans, info= choose(6, 3).run_with_info()
    assert ans == 20
    assert sum(info['stats'].values()) == 0

@task
def task_a():
    def run():
        return None
    return run


@task(compress_level=6)
def task_b():
    def run():
        return None
    return run


@task(compress_level=6)
def task_c():
    @requires(task_a())
    @requires(task_b())
    def run(a: None, b: None):
        return 42
    return run


def test_multiple_tasks():
    task_a.clear()
    task_b.clear()
    task_c.clear()
    assert task_c().run() == 42
