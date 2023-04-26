from checkpoint.graph import task, requires_list


@task(max_concurrency=1)
def choose(n: int, k: int):

    if 0 < k < n:
        @requires_list([choose(n - 1, k - 1), choose(n - 1, k)])
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
    ans = choose(6, 3).run()
    assert ans == 20
    # assert _num_function_calls(choose) == 2 + 4 + 6 + 4 + 2 + 1

    ans = choose(6, 3).run()
    assert ans == 20
    # assert _num_function_calls(choose) == 2 + 4 + 6 + 4 + 2 + 1 + 1

    # TODO: more test
