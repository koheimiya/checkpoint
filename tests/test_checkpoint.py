from checkpoint import checkpoint


@checkpoint()
def choice(n: int, k: int) -> int:
    if 0 < k < n:
        return choice(n - 1, k - 1) + choice(n - 1, k)
    elif k == 0 or k == n:
        return 1
    else:
        raise ValueError(f'n={n}, k={k}')


def test_checkpoint():
    choice.db.clear()
    ans = choice(6, 3)
    assert ans == 20
    assert sum(hit + miss for hit, miss in choice.cache_stats.values()) == 2 + 4 + 6 + 4 + 2 + 1

    ans = choice(6, 3)
    assert ans == 20
    assert sum(hit + miss for hit, miss in choice.cache_stats.values()) == 2 + 4 + 6 + 4 + 2 + 1 + 1

    # import json
    # stats = [(json.loads(k), k) for k, v in choice.cache_stats.items()]
    # stats = {}
    # for key, value in choice.cache_stats.items():
    #     args = json.loads(key)
    #     n = args['n']
    #     k = args['k']
    #     stats[n, k] = value

    # n_max = max(n for n, _ in stats.keys())
    # array = [[(0, 0)] * (1 + i) for i in range(1 + n_max)]
    # for (n, k), (hit, miss) in stats.items():
    #     array[n][k] = (hit, miss)

    # print()
    # for row in array:
    #     print(', '.join(f'{hit}:{miss}' for hit, miss in row))
