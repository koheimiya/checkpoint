from checkpoint import BaseTask, Requires, Const


class Choose(BaseTask[[int, int], int]):
    prev1: Requires[int] = Requires()
    prev2: Requires[int] = Requires()

    def init(self, n: int, k: int):
        if 0 < k < n:
            self.prev1 = Choose(n - 1, k - 1)
            self.prev2 = Choose(n - 1, k)
        else:
            self.prev1 = Const(0)
            self.prev2 = Const(1)

    def main(self) -> int:
        return self.prev1 + self.prev2


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
    Choose.clear_all()
    ans, info = Choose(6, 3).run_with_info(rate_limits={Choose.task__queue: 2})
    assert ans == 20
    assert sum(info['stats'].values()) == 15

    """ 0 caches: """
    ans, info = Choose(6, 3).run_with_info()
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
    Choose(3, 3).clear()
    ans, info = Choose(6, 3).run_with_info()
    assert ans == 20
    assert sum(info['stats'].values()) == 4
