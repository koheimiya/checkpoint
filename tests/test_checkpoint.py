from checkpoint import CHECKPOINT_PATH, checkpoint, _FunctionWithDB


@checkpoint()
def choice(n: int, k: int) -> int:
    if 0 < k < n:
        return choice(n - 1, k - 1) + choice(n - 1, k)
    elif k == 0 or k == n:
        return 1
    else:
        raise ValueError(f'n={n}, k={k}')


def test_checkpoint():
    choice.clear()
    ans = choice(6, 3)
    assert ans == 20
    assert _num_function_calls(choice) == 2 + 4 + 6 + 4 + 2 + 1

    ans = choice(6, 3)
    assert ans == 20
    assert _num_function_calls(choice) == 2 + 4 + 6 + 4 + 2 + 1 + 1


def test_timestamp_management():
    choice.clear()
    ans = choice(6, 3)
    n = _num_function_calls(choice)
    _show_stats(choice)

    choice.delete(3, 3)
    ans2 = choice(6, 3)
    n2 = _num_function_calls(choice)
    _show_stats(choice)

    assert ans == ans2
    assert n2 == n + 7


def _num_function_calls(fn: _FunctionWithDB):
    return sum(hit + miss for hit, miss in fn.cache_stats.values())


def _show_stats(fn: _FunctionWithDB):
    import json
    stats = [(json.loads(k), k) for k, v in fn.cache_stats.items()]
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


@checkpoint(name='the_answer', compress=True)
def task_b():
    task_a()
    return 42


def test_multiple_tasks():
    task_a.clear()
    task_b.clear()
    assert not task_a.cache_stats
    assert task_b() == 42
    assert task_b() == 42
    assert task_a.cache_stats
