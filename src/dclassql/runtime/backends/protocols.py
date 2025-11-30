from __future__ import annotations

import sqlite3
from typing import Callable, Literal, Mapping, Protocol, Sequence, overload, runtime_checkable

from pypika import Query, Table
from pypika.terms import Parameter

from dclassql.model_inspector import DataSourceConfig
from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, WhereT, UpsertWhereT

from .metadata import ColumnSpec, ForeignKeySpec, RelationSpec

ConnectionFactory = Callable[[], sqlite3.Connection]

@runtime_checkable
class TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT](Protocol):
    def __init__(self, backend: BackendProtocol) -> None: ...

    model: type[ModelT]
    insert_model: type[InsertT]
    table_name: str
    datasource: DataSourceConfig
    column_specs: tuple[ColumnSpec, ...]
    column_specs_by_name: Mapping[str, ColumnSpec]

    @classmethod
    def serialize_insert(cls, data: InsertT | Mapping[str, object]) -> dict[str, object]: ...
    @classmethod
    def serialize_update(cls, data: Mapping[str, object]) -> dict[str, object]: ...

    @classmethod
    def deserialize_row(cls, row: Mapping[str, object]) -> ModelT: ...
    primary_key: tuple[str, ...]
    def primary_values(self, instance: ModelT) -> tuple[object, ...]: ...
    indexes: tuple[tuple[str, ...], ...]
    unique_indexes: tuple[tuple[str, ...], ...]
    foreign_keys: tuple[ForeignKeySpec, ...]
    relations: tuple[RelationSpec[TableProtocol], ...]


@runtime_checkable
class BackendProtocol(Protocol):
    quote_char: str
    parameter_token: str
    query_cls: type[Query]
    table_cls: type[Table]
    parameter_cls: type[Parameter]

    def insert(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: InsertT | Mapping[str, object],
    ) -> ModelT: ...
    def update(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT,
        include: IncludeT | None = None,
    ) -> ModelT: ...
    def upsert(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: UpsertWhereT,
        update: Mapping[str, object],
        insert: InsertT | Mapping[str, object],
        include: IncludeT | None = None,
    ) -> ModelT: ...

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]: ...
    @overload
    def update_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT | None = None,
        return_records: Literal[False] = False,
    ) -> int: ...
    @overload
    def update_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT | None = None,
        return_records: Literal[True],
    ) -> list[ModelT]: ...

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: IncludeT | None = None,
        order_by: OrderByT | None = None,
        distinct: Sequence[str] | str | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]: ...

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: IncludeT | None = None,
        order_by: OrderByT | None = None,
        distinct: Sequence[str] | str | None = None,
        skip: int | None = None,
    ) -> ModelT | None: ...

    def delete(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT,
        include: IncludeT | None = None,
    ) -> ModelT | None: ...

    @overload
    def delete_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        return_records: Literal[False] = False,
    ) -> int: ...

    @overload
    def delete_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        return_records: Literal[True],
    ) -> list[ModelT]: ...

    def query_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = False) -> Sequence[dict[str, object]]: ...

    def execute_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = True) -> int: ...

    def escape_identifier(self, name: str) -> str: ...

    def new_parameter(self) -> Parameter: ...
