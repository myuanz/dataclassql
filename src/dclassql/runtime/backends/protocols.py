from __future__ import annotations

import sqlite3
from typing import Callable, Literal, Mapping, Protocol, Sequence, runtime_checkable

from pypika import Query, Table
from pypika.terms import Parameter

from dclassql.model_inspector import DataSourceConfig
from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, WhereT

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
    def deserialize_row(cls, row: Mapping[str, object]) -> ModelT: ...
    primary_key: tuple[str, ...]
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

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]: ...

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: IncludeT | None = None,
        order_by: OrderByT | None = None,
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
        skip: int | None = None,
    ) -> ModelT | None: ...

    def query_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = False) -> Sequence[dict[str, object]]: ...

    def execute_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = True) -> int: ...

    def escape_identifier(self, name: str) -> str: ...

    def new_parameter(self) -> Parameter: ...
