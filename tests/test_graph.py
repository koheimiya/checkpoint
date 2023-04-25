from checkpoint.graph import task, requires


def _choose(n: int, k: int):

    if 0 < k < n:
        @requires(choose(n - 1, k - 1))
        @requires(choose(n - 1, k))
        def runner(x: int, y: int):
            return x + y

    elif k == 0 or k == n:
        def runner() -> int:
            return 1

    else:
        raise ValueError(f'{(n, k)}')
    return runner
choose = task()(_choose)


def test_graph():
    choose.clear()
    ans = choose(6, 3).run()
    assert ans == 20
    # assert _num_function_calls(choose) == 2 + 4 + 6 + 4 + 2 + 1

    ans = choose(6, 3).run()
    assert ans == 20
    # assert _num_function_calls(choose) == 2 + 4 + 6 + 4 + 2 + 1 + 1
