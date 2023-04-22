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
    ans = choice(6, 3)
    assert ans == 20
    print(choice.cache_stats)
