from dataclasses import dataclass
from typing import Any, Generator, Iterable, Literal, Mapping, Self, Type, overload
from weakref import WeakValueDictionary
from typing import get_args


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
        return TableInfo(
            index=indexes,
            primary_key=pk_spec,
            unique_index=unique_indexes
        )



# class _TypedTableGetItemMeta(type):
#     def __getitem__[T](cls, args: tuple[type[T], Any]) -> 'TypedTable[T]':
#         dc, db = args
#         return TypedTable.from_db(dc, db)

# class TypedTable[T](metaclass=_TypedTableGetItemMeta):
#     '''sqlite table with types support, Usage example:

#     ```python
#     from dataclasses import dataclass
#     from typing import reveal_type
#     from fastlite import database
#     from typed_db import TypedTable


#     @dataclass
#     class User:
#         id: int | None
#         name: str
#         email: str


#     user_tb = TypedTable[User, db]
#     '''
#     _tb_cache: WeakValueDictionary[str, 'TypedTable'] = WeakValueDictionary()

#     def __init__(self, table: Any, data_cls: Type[T]):
#         assert hasattr(table, '_orig___call__'), f'{table} 不是一个 fastlite 的表'
#         self._raw_table = table
#         self._dc = data_cls
#         self._table_info = TableInfo.from_dc(data_cls)


#     @property
#     def cls(self) -> type[T]:
#         return self._raw_table.cls

#     @staticmethod
#     def from_db[U](dc: Type[U], db: Any, *, if_not_exists=True, transform=True) -> 'TypedTable[U]':
#         if dc.__name__ in TypedTable._tb_cache:
#             return TypedTable._tb_cache[dc.__name__]
#         table_info = TableInfo.from_dc(dc)

#         tb = db.create(
#             dc, 
#             if_not_exists=if_not_exists,
#             pk=table_info.primary_key.col_name(),
#             transform=transform
#         )
#         for index in table_info.index:
#             index_sqls = tb.create_index(
#                 index.col_name(),
#             )
#             for idx_sql in index_sqls:
#                 db.execute(idx_sql)
#         r = TypedTable(tb, dc)
#         TypedTable._tb_cache[dc.__name__] = r
#         return r

#     @overload
#     def __call__(
#         self, *,
#         where:str|None=None,  # SQL where fragment to use, for example `id > ?`
#         where_args: Iterable|dict|None=None, # Parameters to use with `where`; iterable for `id>?`, or dict for `id>:id`
#         order_by: str|None=None, # Column or fragment of SQL to order by
#         limit:int|None=None, # Number of rows to limit to
#         offset:int|None=None, # SQL offset
#         select:str = "*", # Comma-separated list of columns to select
#         with_pk:bool=False, # Return tuple of (pk,row)?
#         as_cls:bool=True, # Convert returned dict to stored dataclass?
#         xtra:dict|None=None, # Extra constraints
#         fetchone:Literal[False]=..., # Only fetch one result
#         **kwargs
#     )-> Iterable[T]: ...

#     @overload
#     def __call__(
#         self, *,
#         where:str|None=None,  # SQL where fragment to use, for example `id > ?`
#         where_args: Iterable|dict|None=None, # Parameters to use with `where`; iterable for `id>?`, or dict for `id>:id`
#         order_by: str|None=None, # Column or fragment of SQL to order by
#         limit:int|None=None, # Number of rows to limit to
#         offset:int|None=None, # SQL offset
#         select:str = "*", # Comma-separated list of columns to select
#         with_pk:bool=False, # Return tuple of (pk,row)?
#         as_cls:bool=True, # Convert returned dict to stored dataclass?
#         xtra:dict|None=None, # Extra constraints
#         fetchone:Literal[True]=..., # Only fetch one result
#         **kwargs
#     )-> T | None: ...

#     def __call__(
#         self, *,
#         where:str|None=None,  # SQL where fragment to use, for example `id > ?`
#         where_args: Iterable|dict|None=None, # Parameters to use with `where`; iterable for `id>?`, or dict for `id>:id`
#         order_by: str|None=None, # Column or fragment of SQL to order by
#         limit:int|None=None, # Number of rows to limit to
#         offset:int|None=None, # SQL offset
#         select:str = "*", # Comma-separated list of columns to select
#         with_pk:bool=False, # Return tuple of (pk,row)?
#         as_cls:bool=True, # Convert returned dict to stored dataclass?
#         xtra:dict|None=None, # Extra constraints
#         fetchone:bool=False, # Only fetch one result
#         **kwargs
#     )-> Iterable[T] | T | None:
#         if fetchone:
#             limit = 1

#         r = self._raw_table(
#             where=where,
#             where_args=where_args,
#             order_by=order_by,
#             limit=limit,
#             offset=offset,
#             select=select,
#             with_pk=with_pk,
#             as_cls=as_cls,
#             xtra=xtra,
#             # fetchone=fetchone,
#             **kwargs
#         )
#         if fetchone:
#             try:
#                 return next(iter(r))
#             except StopIteration:
#                 return None  # type: ignore
#         else:
#             return r

#     def fetchone(self, **kwargs) -> T | None:
#         '''fastlite 的 fetchone 没有 order_by 参数, 这里转发到自写的 __call__'''
#         return self(**kwargs, fetchone=True)

#     def get(self, pk_values: list|tuple|str|int, as_cls: bool=True, xtra: dict|None=None, default: T|Unset=UNSET) -> T:
#         return self._raw_table.get(pk_values, as_cls=as_cls, xtra=xtra, default=default)
#     @overload
#     def q(self, sql: str, params: Iterable[Any] | Mapping[str, Any] | None = ..., *, as_list: Literal[True]) -> list[dict[str, Any]]: ...
#     @overload
#     def q(self, sql: str, params: Iterable[Any] | Mapping[str, Any] | None = ..., *, as_list: Literal[False]=False) -> Generator[dict[str, Any], None, None]: ...
#     def q(self, sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None, *, as_list: bool = False) -> list[dict[str, Any]] | Generator[dict[str, Any], None, None]:
#         r = self._raw_table.db.query(sql, params)
#         if as_list:
#             return list(r)
#         else:
#             return r

#     def insert_all(
#         self:Self,
#         records: Iterable[dict[str, Any]|T]|None=None, pk=DEFAULT, foreign_keys=DEFAULT,
#         column_order: list[str]|Default|None=DEFAULT,
#         not_null: Iterable[str]|Default|None=DEFAULT,
#         defaults: dict[str, Any]|Default|None=DEFAULT,
#         batch_size=DEFAULT,
#         hash_id: str|Default|None=DEFAULT,
#         hash_id_columns: Iterable[str]|Default|None=DEFAULT,
#         alter: opt_bool=DEFAULT, ignore: opt_bool=DEFAULT, replace: opt_bool=DEFAULT, truncate=False,
#         extracts: dict[str, str]|list[str]|Default|None=DEFAULT,
#         conversions: dict[str, str]|Default|None=DEFAULT,
#         columns: dict[str, Any]|Default|None=DEFAULT,
#         strict: opt_bool=DEFAULT,
#         upsert:bool=False, analyze:bool=False, xtra:dict|None=None,
#         **kwargs
#     ) -> Self:
#         if pk is DEFAULT:
#             pk = self._primary_key

#         return self._raw_table.insert_all(
#             records=records, pk=pk, foreign_keys=foreign_keys,
#             column_order=column_order, not_null=not_null, defaults=defaults,
#             batch_size=batch_size, hash_id=hash_id, hash_id_columns=hash_id_columns,
#             alter=alter, ignore=ignore, replace=replace, truncate=truncate,
#             extracts=extracts, conversions=conversions, columns=columns,
#             strict=strict, upsert=upsert, analyze=analyze, xtra=xtra,
#             **kwargs
#         )

#     def insert(
#         self,
#         record: dict[str, Any]|T, pk=DEFAULT, foreign_keys=DEFAULT,
#         column_order: list[str]|Default|None=DEFAULT,
#         not_null: Iterable[str]|Default|None=DEFAULT,
#         defaults: dict[str, Any]|Default|None=DEFAULT,
#         hash_id: str|Default|None=DEFAULT,
#         hash_id_columns: Iterable[str]|Default|None=DEFAULT,
#         alter: opt_bool=DEFAULT,
#         ignore: opt_bool=DEFAULT,
#         replace: opt_bool=DEFAULT,
#         extracts: dict[str, str]|list[str]|Default|None=DEFAULT,
#         conversions: dict[str, str]|Default|None=DEFAULT,
#         columns: dict[str, Any]|Default|None=DEFAULT,
#         strict: opt_bool=DEFAULT,
#         **kwargs
#     ) -> T:
#         return self._raw_table.insert(
#             record=record, pk=pk, foreign_keys=foreign_keys,
#             column_order=column_order, not_null=not_null, defaults=defaults,
#             hash_id=hash_id, hash_id_columns=hash_id_columns,
#             alter=alter, ignore=ignore, replace=replace,
#             extracts=extracts, conversions=conversions, columns=columns,
#             strict=strict, **kwargs
#         )

#     def where(self, **kwargs) -> Iterable[T]:
#         return self(where=' AND '.join(f'{k}=?' for k in kwargs.keys()), where_args=tuple(kwargs.values()))
#     def where_one(self, **kwargs) -> T | None:
#         return self(where=' AND '.join(f'{k}=?' for k in kwargs.keys()), where_args=tuple(kwargs.values()), fetchone=True)

#     def delete(self, id: Any) -> int:
#         return self._raw_table.delete(id)

#     def __repr__(self) -> str:
#         return f'{self._raw_table.__repr__()} | TypedTable'

#     def __str__(self) -> str:
#         return self._raw_table.__str__()
