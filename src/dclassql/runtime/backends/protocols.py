from __future__ import annotations

import sqlite3
from typing import Callable, Literal, Mapping, Protocol, Sequence, runtime_checkable

from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, WhereT

from .metadata import ColumnSpec, ForeignKeySpec, RelationSpec

ConnectionFactory = Callable[[], sqlite3.Connection]

@runtime_checkable
class TableProtocol[
    ModelT,
    InsertT,
    WhereT: Mapping[str, object],
    IncludeT: Mapping[str, bool],
    OrderByT: Mapping[str, Literal['asc', 'desc']],
](Protocol):
    def __init__(self, backend: BackendProtocol) -> None: ...

    model: type[ModelT]
    insert_model: type[InsertT]
    column_specs: tuple[ColumnSpec, ...]
    column_specs_by_name: Mapping[str, ColumnSpec]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKeySpec, ...]
    relations: tuple[RelationSpec[BackendProtocol], ...]


@runtime_checkable
class BackendProtocol(Protocol):
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

    def query_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = False) -> Sequence[object]: ...

    def execute_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = True) -> int: ...
