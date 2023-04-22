# from checkpoint.checkpoint_legacy import checkpoint, _FunctionWithDB
from checkpoint import checkpoint, FunctionWithDB


@checkpoint()
def choose(n: int, k: int) -> int:
    if 0 < k < n:
        return choose(n - 1, k - 1) + choose(n - 1, k)
    elif k == 0 or k == n:
        return 1
    else:
        raise ValueError(f'n={n}, k={k}')


def test_checkpoint():
    choose.clear()
    ans = choose(6, 3)
    assert ans == 20
    assert _num_function_calls(choose) == 2 + 4 + 6 + 4 + 2 + 1

    ans = choose(6, 3)
    assert ans == 20
    assert _num_function_calls(choose) == 2 + 4 + 6 + 4 + 2 + 1 + 1


def test_timestamp_management():
    choose.clear()
    ans = choose(6, 3)
    n = _num_function_calls(choose)
    _show_stats(choose)

    choose.delete(3, 3)
    ans2 = choose(6, 3)
    n2 = _num_function_calls(choose)
    _show_stats(choose)

    assert ans == ans2
    assert n2 == n + 7


def _num_function_calls(fn: FunctionWithDB):
    return sum(hit + miss for hit, miss in fn.cache_stats.values())


def _show_stats(fn: FunctionWithDB):
    import json
    stats = [(json.loads(k), k) for k, _ in fn.cache_stats.items()]
    stats = {}
    for key, value in fn.cache_stats.items():
        args = json.loads(key)
        n = args['n']
        k = args['k']
        stats[n, k] = value

    n_max = max(n for n, _ in stats.keys())
    array = [[(0, 0)] * (1 + i) for i in range(1 + n_max)]
    for (n, k), (hit, miss) in stats.items():
        array[n][k] = (hit, miss)

    print()
    for row in array:
        print(', '.join(f'{hit}:{miss}' for hit, miss in row))


@checkpoint()
def task_a():
    return None


@checkpoint(compress_level=6)
def task_b():
    return None


@checkpoint(name='the_answer', compress_level=6)
def task_c():
    task_a()
    task_b()
    return 42


def test_multiple_tasks():
    task_a.clear()
    task_b.clear()
    task_c.clear()
    assert not task_a.cache_stats
    assert task_c() == 42
    assert task_c() == 42
    assert task_a.cache_stats
