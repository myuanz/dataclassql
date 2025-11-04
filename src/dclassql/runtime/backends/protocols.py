from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

ConnectionFactory = Callable[[], sqlite3.Connection]

@dataclass(slots=True)
class RelationSpec:
    name: str
    table_name: str
    table_module: str
    many: bool
    mapping: tuple[tuple[str, str], ...]


@runtime_checkable
class TableProtocol[ModelT, InsertT, WhereT](Protocol):
    def __init__(self, backend: "BackendProtocol[ModelT, InsertT, WhereT]") -> None: ...

    model: type[ModelT]
    insert_model: type[InsertT]
    columns: tuple[str, ...]
    auto_increment_columns: tuple[str, ...]
    primary_key: tuple[str, ...]
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
