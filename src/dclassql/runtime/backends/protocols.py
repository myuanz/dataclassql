from __future__ import annotations

import sqlite3
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

from .metadata import ColumnSpec, ForeignKeySpec, RelationSpec

ConnectionFactory = Callable[[], sqlite3.Connection]

@runtime_checkable
class TableProtocol[ModelT, InsertT, WhereT](Protocol):
    def __init__(self, backend: "BackendProtocol[ModelT, InsertT, WhereT]") -> None: ...

    model: type[ModelT]
    insert_model: type[InsertT]
    column_specs: tuple[ColumnSpec, ...]
    column_specs_by_name: Mapping[str, ColumnSpec]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKeySpec, ...]
    relations: tuple[RelationSpec, ...]


@runtime_checkable
class BackendProtocol[ModelT, InsertT, WhereT](Protocol):
    def insert(self, table: TableProtocol[ModelT, InsertT, WhereT], data: InsertT | Mapping[str, object]) -> ModelT: ...

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]: ...

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]: ...

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        skip: int | None = None,
    ) -> ModelT | None: ...
