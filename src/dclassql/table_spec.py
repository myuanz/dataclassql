from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Self, Type, overload
from weakref import WeakValueDictionary
from typing import get_args
from types import GeneratorType


@dataclass
class Col:
    name: str
    table: Type

@dataclass
class KeySpec:
    cols: tuple[Col, ...] | Col

    is_primary: bool = False
    is_index: bool = False
    is_unique_index: bool = False
    is_auto_increment: bool = False

    def unique(self):
        self.is_unique_index = True
        return self
    def index(self):
        self.is_index = True
        return self
    def primary(self):
        self.is_primary = True
        return self
    def auto_increment(self):
        self.is_auto_increment = True
        return self


    def col_name(self) -> str | tuple[str, ...]:
        if isinstance(self.cols, tuple):
            return tuple(col.name for col in self.cols)
        else:
            return self.cols.name

def KS(*cols: Col | Any) -> KeySpec:
    assert all(isinstance(col, Col) for col in cols), 'The arguments to KS must be Col instances'
    normalized: tuple[Col, ...] | Col
    if len(cols) == 1:
        normalized = cols[0]
    else:
        normalized = tuple(cols)  # type: ignore[assignment]
    return KeySpec(cols=normalized)

class FakeSelf:
    def __init__(self, tb: Type) -> None:
        self.tb = tb
    def __getattr__(self, name: str) -> Col:
        return Col(name, table=self.tb)

@dataclass
class TableInfo:
    index: list[KeySpec]
    primary_key: KeySpec
    unique_index: list[KeySpec]

    @staticmethod
    def _coerce_cols(value: Any) -> Col | tuple[Col, ...]:
        if isinstance(value, Col):
            return value
        if isinstance(value, tuple) and value and all(isinstance(col, Col) for col in value):
            return value
        if isinstance(value, Iterable):
            collected = [item for item in value]
            if not collected:
                raise ValueError('Primary key/index specification cannot be empty')
            if not all(isinstance(col, Col) for col in collected):
                raise TypeError('Primary key/index specification must be Col instances')
            if len(collected) == 1:
                return collected[0]
            return tuple(collected)  # type: ignore[return-value]
        raise TypeError(f'Unsupported specification type: {type(value)!r}')

    @staticmethod
    def _resolve_primary_key(default_pk: KeySpec, value: Any) -> KeySpec:
        if isinstance(value, GeneratorType):
            raise TypeError('primary_key() must return a KeySpec or Col(s), not a generator. May be you meant to use "return" instead?')
        if isinstance(value, KeySpec):
            if not value.is_primary:
                value.primary()
            return value
        cols = TableInfo._coerce_cols(value)
        default_pk.cols = cols
        return default_pk

    @staticmethod
    def _iter_index_specs(raw: Any) -> Iterable[Any]:
        if isinstance(raw, (KeySpec, Col)):
            yield raw
            return
        if isinstance(raw, tuple) and raw and all(isinstance(col, Col) for col in raw):
            yield raw
            return
        if isinstance(raw, Iterable):
            for item in raw:
                yield from TableInfo._iter_index_specs(item)
            return
        raise TypeError(f'Unsupported index specification: {raw!r}')

    @staticmethod
    def _normalize_index_spec(value: Any) -> KeySpec:
        if isinstance(value, KeySpec):
            if not value.is_primary:
                value.index()
            return value
        cols = TableInfo._coerce_cols(value)
        if isinstance(cols, tuple):
            return KS(*cols).index()
        return KS(cols).index()

    @staticmethod
    def from_dc(dc: type) -> 'TableInfo':
        pk_spec = KS(Col('id', table=dc)).primary()

        fake_self = FakeSelf(dc)
        if hasattr(dc, 'primary_key'):
            pk = getattr(dc, 'primary_key')(fake_self)
            pk_spec = TableInfo._resolve_primary_key(pk_spec, pk)

        indexes: list[KeySpec] = []
        if hasattr(dc, 'index'):
            raw_indexes = getattr(dc, 'index')(fake_self)
            if raw_indexes is not None:
                for idx in TableInfo._iter_index_specs(raw_indexes):
                    indexes.append(TableInfo._normalize_index_spec(idx))
        unique_indexes: list[KeySpec] = []
        if hasattr(dc, 'unique_index'):
            raw_unique_indexes = getattr(dc, 'unique_index')(fake_self)
            if raw_unique_indexes is not None:
                for uidx in TableInfo._iter_index_specs(raw_unique_indexes):
                    spec = TableInfo._normalize_index_spec(uidx)
                    spec.unique()
                    unique_indexes.append(spec)
        indexes.extend(unique_indexes)
        return TableInfo(
            index=indexes,
            primary_key=pk_spec,
            unique_index=unique_indexes
        )
