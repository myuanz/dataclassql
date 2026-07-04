from dclassql.db_pool import BaseDBPool, save_local


class CounterPool(BaseDBPool):
    def __init__(self, label: str) -> None:
        self.label = label
        self.calls = 0

    @save_local
    def default_value(self) -> tuple[str, int]:
        self.calls += 1
        return self.label, self.calls

    @save_local(key=lambda self, func: (func.__name__, self.label))
    def keyed_value(self) -> tuple[str, int]:
        self.calls += 1
        return self.label, self.calls


def test_save_local_supports_bare_decorator() -> None:
    pool = CounterPool("a")

    assert pool.default_value() == ("a", 1)
    assert pool.default_value() == ("a", 1)
    assert pool.calls == 1

    CounterPool.close_all()


def test_save_local_supports_keyed_decorator() -> None:
    first = CounterPool("first")
    second = CounterPool("second")

    assert first.keyed_value() == ("first", 1)
    assert first.keyed_value() == ("first", 1)
    assert second.keyed_value() == ("second", 1)

    CounterPool.close_all()
