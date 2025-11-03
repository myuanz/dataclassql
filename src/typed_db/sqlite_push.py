from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

from pypika import Query, Table
from pypika.queries import Column
from pypika.utils import format_quotes

from .model_inspector import ModelInfo, inspect_models
from .table_spec import TableInfo


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


def _normalize_col_names(spec_cols: Any) -> tuple[str, ...]:
    if isinstance(spec_cols, tuple):
        return tuple(spec_cols)
    if isinstance(spec_cols, list):
        return tuple(spec_cols)
    return (spec_cols,)


def _render_create_table_sql(info: ModelInfo, table_info: TableInfo) -> str:
    table_name = info.model.__name__
    builder = Query.create_table(table_name).if_not_exists()

    pk_cols = _normalize_col_names(table_info.primary_key.col_name())
    pk_set = set(pk_cols)
    single_auto_inc = (
        len(pk_cols) == 1
        and any(col.name == pk_cols[0] and col.auto_increment for col in info.columns)
    )

    for column in info.columns:
        sql_type = _infer_sqlite_type(column.python_type)
        col_def = sql_type
        is_pk_single = len(pk_cols) == 1 and column.name == pk_cols[0]
        if is_pk_single and column.auto_increment and sql_type.upper() == "INTEGER":
            col_def = f"{sql_type} PRIMARY KEY AUTOINCREMENT"
        else:
            if not column.optional and column.name not in pk_set:
                col_def = f"{col_def} NOT NULL"
        builder = builder.columns(Column(column.name, col_def))

    if pk_cols:
        if len(pk_cols) == 1 and not single_auto_inc:
            builder = builder.primary_key(*pk_cols)
        elif len(pk_cols) > 1:
            builder = builder.primary_key(*pk_cols)

    unique_seen: set[tuple[str, ...]] = set()
    for spec in table_info.unique_index:
        cols = _normalize_col_names(spec.col_name())
        if cols in unique_seen:
            continue
        unique_seen.add(cols)
        builder = builder.unique(*cols)

    return builder.get_sql(quote_char='"') + ';'


def _render_index_sql(
    table: str,
    cols: Iterable[str],
    *,
    unique: bool,
) -> tuple[str, str] | None:
    cols_tuple = tuple(cols)
    if not cols_tuple:
        return None
    index_name = f'{"uq" if unique else "idx"}_{table}_{"_".join(cols_tuple)}'
    table_ref = Table(table)
    columns_clause = ", ".join(
        table_ref.field(col).get_sql(quote_char='"') for col in cols_tuple
    )
    unique_kw = "UNIQUE " if unique else ""
    sql = (
        f"CREATE {unique_kw}INDEX IF NOT EXISTS "
        f"{format_quotes(index_name, '"')} ON {table_ref.get_sql(quote_char='"')} ({columns_clause});"
    )
    return index_name, sql


def _apply_sql_statements(conn: sqlite3.Connection, statements: Iterable[str]) -> None:
    for sql in statements:
        conn.execute(sql)
    conn.commit()


def push_sqlite(
    conn: sqlite3.Connection,
    infos: Sequence[ModelInfo],
    *,
    sync_indexes: bool = False,
) -> None:
    for info in infos:
        create_sql, index_entries = _build_sqlite_schema(info)

        statements: list[str] = [create_sql]
        expected_indexes = {name for name, _ in index_entries}

        existing_indexes = set()
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('index','unique') "
            "AND tbl_name = ?",
            (info.model.__name__,),
        )
        existing_indexes.update(name for (name,) in cur.fetchall())

        if sync_indexes:
            for name in sorted(existing_indexes):
                if name.startswith("sqlite_"):
                    continue
                if name in expected_indexes:
                    continue
                statements.append(
                    f"DROP INDEX IF EXISTS {format_quotes(name, '\"')};"
                )

        for name, sql in index_entries:
            if name in existing_indexes:
                continue
            statements.append(sql)

        _apply_sql_statements(conn, statements)


def db_push(
    models: Sequence[type[Any]],
    connections: Mapping[str, Any],
    *,
    sync_indexes: bool = False,
) -> None:
    model_infos = inspect_models(models)
    grouped: dict[str, list[ModelInfo]] = {}
    for info in model_infos.values():
        grouped.setdefault(info.datasource.provider, []).append(info)

    for provider, infos in grouped.items():
        if provider != "sqlite":
            raise ValueError(f"Unsupported provider: {provider}")
        if provider not in connections:
            raise KeyError(f"Connection for provider '{provider}' is missing")
        conn = connections[provider]
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError(
                f"Connection for provider '{provider}' must be sqlite3.Connection"
            )
        push_sqlite(conn, infos, sync_indexes=sync_indexes)


def _build_sqlite_schema(info: ModelInfo) -> tuple[str, list[tuple[str, str]]]:
    table_info = TableInfo.from_dc(info.model)
    create_sql = _render_create_table_sql(info, table_info)
    index_entries: list[tuple[str, str]] = []
    seen_unique: set[tuple[str, ...]] = set()

    for spec in table_info.index:
        cols = _normalize_col_names(spec.col_name())
        is_unique = spec.is_unique_index
        if is_unique:
            if cols in seen_unique:
                continue
            seen_unique.add(cols)
        entry = _render_index_sql(
            info.model.__name__,
            cols,
            unique=is_unique,
        )
        if entry:
            index_entries.append(entry)

    return create_sql, index_entries
