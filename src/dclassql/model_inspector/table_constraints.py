from dataclasses import dataclass
from types import GeneratorType
from typing import Any, Iterable, Self


@dataclass(slots=True, frozen=True)
class Col:
    name: str


@dataclass(slots=True, frozen=True)
class ColGroup:
    cols: tuple[Col, ...]

    def __post_init__(self) -> None:
        if not self.cols:
            raise ValueError('ColGroup cannot be empty')
        if not all(isinstance(col, Col) for col in self.cols):
            raise TypeError('ColGroup cols must be Col instances')

    @classmethod
    def from_cols(cls, *cols: Col) -> Self:
        return cls(cols=tuple(cols))

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.cols)

class FakeSelf:
    def __getattr__(self, name: str) -> Col:
        return Col(name)

@dataclass(slots=True, frozen=True)
class TableConstraints:
    primary_key: ColGroup
    indexes: tuple[ColGroup, ...]
    unique_indexes: tuple[ColGroup, ...]

    def is_unique(self, columns: Iterable[str]) -> bool:
        column_names = tuple(columns)
        groups = (self.primary_key, *self.unique_indexes)
        return any(
            len(group.names) == len(column_names)
            and set(group.names) == set(column_names)
            for group in groups
        )

    @staticmethod
    def _coerce_cols(value: Any) -> tuple[Col, ...]:
        if isinstance(value, Col):
            return (value,)
        if isinstance(value, tuple) and value and all(isinstance(col, Col) for col in value):
            return value
        if isinstance(value, Iterable):
            collected = tuple(value)
            if not collected:
                raise ValueError('Primary key/index specification cannot be empty')
            if not all(isinstance(col, Col) for col in collected):
                raise TypeError('Primary key/index specification must be Col instances')
            return collected
        raise TypeError(f'Unsupported specification type: {type(value)!r}')

    @staticmethod
    def _resolve_primary_key(value: Any) -> ColGroup:
        if isinstance(value, GeneratorType):
            raise TypeError('primary_key() must return a ColGroup or Col(s), not a generator. May be you meant to use "return" instead?')
        if isinstance(value, ColGroup):
            return value
        return ColGroup(TableConstraints._coerce_cols(value))

    @staticmethod
    def _iter_index_specs(raw: Any) -> Iterable[Any]:
        if isinstance(raw, (ColGroup, Col)):
            yield raw
            return
        if isinstance(raw, tuple) and raw and all(isinstance(col, Col) for col in raw):
            yield raw
            return
        if isinstance(raw, Iterable):
            for item in raw:
                yield from TableConstraints._iter_index_specs(item)
            return
        raise TypeError(f'Unsupported index specification: {raw!r}')

    @staticmethod
    def _normalize_index_spec(value: Any) -> ColGroup:
        if isinstance(value, ColGroup):
            return value
        return ColGroup(TableConstraints._coerce_cols(value))

    @staticmethod
    def from_dc(dc: type) -> 'TableConstraints':
        primary_key = ColGroup.from_cols(Col('id'))

        fake_self = FakeSelf()
        if hasattr(dc, 'primary_key'):
            pk = getattr(dc, 'primary_key')(fake_self)
            primary_key = TableConstraints._resolve_primary_key(pk)

        indexes: list[ColGroup] = []
        if hasattr(dc, 'index'):
            raw_indexes = getattr(dc, 'index')(fake_self)
            if raw_indexes is not None:
                for idx in TableConstraints._iter_index_specs(raw_indexes):
                    indexes.append(TableConstraints._normalize_index_spec(idx))
        unique_indexes: list[ColGroup] = []
        if hasattr(dc, 'unique_index'):
            raw_unique_indexes = getattr(dc, 'unique_index')(fake_self)
            if raw_unique_indexes is not None:
                for uidx in TableConstraints._iter_index_specs(raw_unique_indexes):
                    unique_indexes.append(TableConstraints._normalize_index_spec(uidx))
        return TableConstraints(
            primary_key=primary_key,
            indexes=tuple(indexes),
            unique_indexes=tuple(unique_indexes),
        )
