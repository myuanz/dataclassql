from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

from pypika import Query, Table
from pypika.utils import format_quotes

from ..model_inspector import TypeHint
from ..runtime.backends.metadata import ColumnSpec
from ..runtime.backends.protocols import SchemaTableProtocol
from .base import (
    ColumnDeclaration,
    ConfirmRebuildCallback,
    DatabasePusher,
    ExistingColumn,
    SchemaBuilder,
    SchemaDiff,
    SchemaPlan,
)


TYPE_MAP: Mapping[type[Any], str] = {
    int: "INTEGER",
    bool: "INTEGER",
    float: "REAL",
    str: "TEXT",
    datetime: "datetime",
    date: "date",
    bytes: "BLOB",
}


def _infer_sqlite_type(annotation: Any) -> str:
    type_hint = TypeHint(annotation).without_transparent_wrappers()
    annotation = type_hint.source
    origin = type_hint.origin
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
    return "TEXT"


class SQLiteSchemaBuilder(SchemaBuilder):
    quote_char = '"'

    def resolve_column_type(self, annotation: Any) -> str:
        return _infer_sqlite_type(annotation)

    def render_column_declaration(
        self,
        *,
        column: ColumnSpec,
        pk_columns: tuple[str, ...],
        pk_members: set[str],
        single_inline_pk: bool,
    ) -> ColumnDeclaration:
        if column.storage_kind == "json":
            return self._build_json_column(column, pk_members)
        return super().render_column_declaration(
            column=column,
            pk_columns=pk_columns,
            pk_members=pk_members,
            single_inline_pk=single_inline_pk,
        )

    def _build_json_column(self, column: ColumnSpec, pk_members: set[str]) -> ColumnDeclaration:
        primary_key = column.name in pk_members
        not_null = not column.optional and not primary_key
        definition_sql = "TEXT"
        if not_null:
            definition_sql += " NOT NULL"
        return ColumnDeclaration(
            name=column.name,
            type_sql="TEXT",
            definition_sql=definition_sql,
            not_null=not_null,
            primary_key=primary_key,
            auto_increment=False,
        )

    def use_inline_primary_key(
        self,
        *,
        column: ColumnSpec,
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

    def table_exists(self, conn: sqlite3.Connection, table: SchemaTableProtocol) -> bool:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table.table_name,),
        )
        return cur.fetchone() is not None

    def inspect_existing_schema(
        self,
        conn: sqlite3.Connection,
        table: SchemaTableProtocol,
    ) -> tuple[ExistingColumn, ...] | None:
        table_name = table.table_name
        quoted = format_quotes(table_name, '"')
        rows = conn.execute(f"PRAGMA table_info({quoted})").fetchall()
        if not rows:
            return None
        columns = [
            ExistingColumn(
                name=row[1],
                type_sql=row[2],
                not_null=bool(row[3]),
                primary_key=bool(row[5]),
            )
            for row in rows
        ]
        return tuple(columns)

    def fetch_existing_indexes(self, conn: sqlite3.Connection, table: SchemaTableProtocol) -> set[str]:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('index','unique') AND tbl_name = ?",
            (table.table_name,),
        )
        return {name for (name,) in cur.fetchall()}

    def execute_statements(self, conn: sqlite3.Connection, statements: Iterable[str]) -> None:
        for sql in statements:
            conn.execute(sql)
        conn.commit()

    def is_system_index(self, name: str) -> bool:
        return name.startswith("sqlite_")

    def rebuild_table(
        self,
        conn: sqlite3.Connection,
        table: SchemaTableProtocol,
        builder: SchemaBuilder,
        plan: SchemaPlan,
        existing_schema: tuple[ExistingColumn, ...] | None,
        diff: SchemaDiff,
    ) -> None:
        table_name = builder.table_name
        temp_table = f"{table_name}__tmp"

        temp_plan = builder.build(table_name=temp_table)

        statements: list[str] = []
        drop_temp_sql = Query.drop_table(temp_table).if_exists().get_sql(quote_char=builder.quote_char) + ';'
        statements.append(drop_temp_sql)
        statements.append(temp_plan.create_sql)

        existing_columns = {column.name for column in existing_schema or ()}
        transfer_columns = [
            declaration.name
            for declaration in plan.columns
            if declaration.name in existing_columns
        ]

        if transfer_columns:
            temp_ref = Table(temp_table)
            original_ref = Table(table_name)
            insert_sql = (
                Query.into(temp_ref)
                .columns(*transfer_columns)
                .from_(original_ref)
                .select(*(original_ref.field(name) for name in transfer_columns))
                .get_sql(quote_char=builder.quote_char)
                + ';'
            )
            statements.append(insert_sql)

        drop_original_sql = Query.drop_table(table_name).if_exists().get_sql(quote_char=builder.quote_char) + ';'
        statements.append(drop_original_sql)

        rename_sql = (
            f"ALTER TABLE {format_quotes(temp_table, builder.quote_char)} "
            f"RENAME TO {format_quotes(table_name, builder.quote_char)};"
        )
        statements.append(rename_sql)

        self.execute_statements(conn, statements)


SQLITE_PUSHER = SQLitePusher()


def _build_sqlite_schema(table: SchemaTableProtocol) -> tuple[str, list[tuple[str, str]]]:
    builder = SQLiteSchemaBuilder(table)
    plan = builder.build()
    index_entries: list[tuple[str, str]] = [
        (definition.name, builder.create_index_sql(definition)) for definition in plan.indexes
    ]
    return plan.create_sql, index_entries


def push_sqlite(
    conn: sqlite3.Connection,
    tables: Sequence[SchemaTableProtocol],
    *,
    sync_indexes: bool = False,
    confirm_rebuild: ConfirmRebuildCallback | None = None,
) -> None:
    SQLITE_PUSHER.push(conn, tables, sync_indexes=sync_indexes, confirm_rebuild=confirm_rebuild)
