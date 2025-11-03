from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

from ..model_inspector import ColumnInfo, ModelInfo
from .base import DatabasePusher, SchemaBuilder


TYPE_MAP: Mapping[type[Any], str] = {
    int: "INTEGER",
    bool: "INTEGER",
    float: "REAL",
    str: "TEXT",
    datetime: "TEXT",
    date: "TEXT",
    bytes: "BLOB",
}


def _infer_sqlite_type(annotation: Any) -> str:
    origin = getattr(annotation, "__origin__", None)
    if origin is None and isinstance(annotation, type):
        if annotation in TYPE_MAP:
            return TYPE_MAP[annotation]
        if issubclass(annotation, str):
            return "TEXT"
        if issubclass(annotation, bytes):
            return "BLOB"
        if issubclass(annotation, int):
            return "INTEGER"
        if issubclass(annotation, float):
            return "REAL"
    if origin in (list, set, tuple):
        return "TEXT"
    if origin is None and annotation.__class__ is type:
        return TYPE_MAP.get(annotation, "TEXT")
    return "TEXT"


class SQLiteSchemaBuilder(SchemaBuilder):
    quote_char = '"'

    def resolve_column_type(self, annotation: Any) -> str:
        return _infer_sqlite_type(annotation)

    def use_inline_primary_key(
        self,
        *,
        column: ColumnInfo,
        pk_columns: tuple[str, ...],
        sql_type: str,
    ) -> bool:
        if len(pk_columns) != 1:
            return False
        if column.name != pk_columns[0]:
            return False
        if not column.auto_increment:
            return False
        return sql_type.upper() == "INTEGER"

    def inline_primary_key_definition(self, sql_type: str) -> str:
        return f"{sql_type} PRIMARY KEY AUTOINCREMENT"


class SQLitePusher(DatabasePusher):
    schema_builder_cls = SQLiteSchemaBuilder

    def validate_connection(self, conn: Any) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("SQLite connections must be sqlite3.Connection")

    def fetch_existing_indexes(self, conn: sqlite3.Connection, info: ModelInfo) -> set[str]:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('index','unique') AND tbl_name = ?",
            (info.model.__name__,),
        )
        return {name for (name,) in cur.fetchall()}

    def execute_statements(self, conn: sqlite3.Connection, statements: Iterable[str]) -> None:
        for sql in statements:
            conn.execute(sql)
        conn.commit()

    def is_system_index(self, name: str) -> bool:
        return name.startswith("sqlite_")


SQLITE_PUSHER = SQLitePusher()


def _build_sqlite_schema(info: ModelInfo) -> tuple[str, list[tuple[str, str]]]:
    builder = SQLiteSchemaBuilder(info)
    create_sql, index_definitions = builder.build()
    index_entries: list[tuple[str, str]] = [
        (definition.name, builder.create_index_sql(definition)) for definition in index_definitions
    ]
    return create_sql, index_entries


def push_sqlite(
    conn: sqlite3.Connection,
    infos: Sequence[ModelInfo],
    *,
    sync_indexes: bool = False,
) -> None:
    SQLITE_PUSHER.push(conn, infos, sync_indexes=sync_indexes)
