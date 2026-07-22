from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, cast, overload

from .protocols import BackendProtocol


@dataclass(slots=True, frozen=True)
class LazyLookupKey:
    backend: BackendProtocol
    table_cls: type[Any]
    criteria: tuple[tuple[str, object], ...] | None
    many: bool

    def _table(self) -> Any:
        return self.table_cls(self.backend)

    def _where(self) -> dict[str, object] | None:
        return None if self.criteria is None else dict(self.criteria)

    def find_first(self, *, skip: int | None = None) -> Any:
        where = self._where()
        if where is None:
            return None
        return self.backend.find_first(
            self._table(),
            where=cast(Any, where),
            skip=skip,
        )

    def find_many(self) -> list[Any]:
        where = self._where()
        if where is None:
            return []
        return self.backend.find_many(
            self._table(),
            where=cast(Any, where),
        )

    def count(self) -> int:
        where = self._where()
        if where is None:
            return 0
        return self.backend.count(
            self._table(),
            where=cast(Any, where),
        )

    def resolve(self) -> Any:
        if self.many:
            return self.find_many()
        return self.find_first()


class _LazyLookupProxy:
    __slots__ = ()

    @property
    def lookup_key(self) -> LazyLookupKey:
        query = object.__getattribute__(self, "_lazy_query")
        return cast(LazyLookupKey, query.lookup_key)

    @property
    def _comparison_materializer(self) -> str:
        return "eager(value)"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _LazyLookupProxy):
            return self.lookup_key == other.lookup_key
        raise TypeError(
            "Lazy relation comparison requires another proxy; "
            f"materialize it with {self._comparison_materializer} first"
        )

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self.lookup_key)


class LazyRelationView[T](_LazyLookupProxy, Sequence[T]):
    __slots__ = ("attribute", "_lookup_key", "_snapshot")

    def __init__(
        self,
        attribute: str,
        lookup_key: LazyLookupKey,
    ) -> None:
        if not lookup_key.many:
            raise TypeError("LazyRelationView requires a many-valued lookup")
        self.attribute = attribute
        self._lookup_key = lookup_key
        self._snapshot: tuple[T, ...] | None = None

    @property
    def lookup_key(self) -> LazyLookupKey:
        return self._lookup_key

    @property
    def _comparison_materializer(self) -> str:
        return "list(value)"

    def _resolve_all(self) -> tuple[T, ...]:
        if self._snapshot is None:
            self._snapshot = tuple(cast(Sequence[T], self.lookup_key.find_many()))
        return self._snapshot

    def __bool__(self) -> bool:
        if self._snapshot is not None:
            return bool(self._snapshot)
        return self.lookup_key.find_first() is not None

    def __len__(self) -> int:
        if self._snapshot is not None:
            return len(self._snapshot)
        return self.lookup_key.count()

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[T]: ...

    def __getitem__(self, index: int | slice) -> T | Sequence[T]:
        if self._snapshot is not None or isinstance(index, slice) or index < 0:
            return self._resolve_all()[index]
        value = self.lookup_key.find_first(skip=index)
        if value is None:
            raise IndexError(index)
        return cast(T, value)

    def __iter__(self) -> Iterator[T]:
        return iter(self._resolve_all())

    def __reversed__(self) -> Iterator[T]:
        return reversed(self._resolve_all())

    def __contains__(self, value: object) -> bool:
        return value in self._resolve_all()

    def index(self, value: T, start: int = 0, stop: int | None = None) -> int:
        snapshot = self._resolve_all()
        if stop is None:
            return snapshot.index(value, start)
        return snapshot.index(value, start, stop)

    def count(self, value: T) -> int:
        return self._resolve_all().count(value)

    def __repr__(self) -> str:
        if self._snapshot is None:
            return f"<LazyRelationView {self.attribute} (lazy)>"
        return f"LazyRelationView({self._snapshot!r})"

    def __str__(self) -> str:
        return self.__repr__()


__all__ = ["LazyLookupKey", "LazyRelationView"]
