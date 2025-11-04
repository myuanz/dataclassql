from __future__ import annotations

import sqlite3
import threading
from typing import Any, Mapping, Sequence, cast

from pypika.dialects import SQLLiteQuery
from .base import BackendBase
from .protocols import BackendProtocol, ConnectionFactory, TableProtocol


class SQLiteBackend[ModelT, InsertT, WhereT: Mapping[str, object]](BackendBase[ModelT, InsertT, WhereT]):
    query_cls = SQLLiteQuery

    def __init__(self, source: sqlite3.Connection | ConnectionFactory | "SQLiteBackend") -> None:
        super().__init__()
        if isinstance(source, SQLiteBackend):
            self._factory: ConnectionFactory | None = source._factory
            self._connection: sqlite3.Connection | None = source._connection
            self._local = source._local
            self._identity_map = source._identity_map
        elif isinstance(source, sqlite3.Connection):
            self._factory = None
            self._connection = source
            self._ensure_row_factory(self._connection)
            self._local = threading.local()
        elif callable(source):
            self._factory = source
            self._connection = None
            self._local = threading.local()
        else:
            raise TypeError("SQLite backend source must be connection or callable returning connection")

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]:
        items = list(data)
        if not items:
            return []

        payloads = [self._normalize_insert_payload(table, item) for item in items]
        columns = list(payloads[0].keys())
        if not columns:
            raise ValueError("Insert payload cannot be empty")

        params_matrix = [[payload.get(column) for column in columns] for payload in payloads]
        table_name = table.model.__name__
        sql_table = self.table_cls(table_name)
        insert_query = (
            self.query_cls.into(sql_table)
            .columns(*columns)
            .insert(*(self._new_parameter() for _ in columns))
        )
        sql = self._render_query(insert_query)

        pk_columns = list(table.primary_key)
        if not pk_columns:
            raise ValueError(f"Table {table_name} does not define primary key")
        auto_increment = set(table.auto_increment_columns)

        results: list[ModelT] = []
        step = batch_size if batch_size and batch_size > 0 else len(payloads)
        start = 0
        connection = self._acquire_connection()
        while start < len(payloads):
            end = min(start + step, len(payloads))
            subset_payloads = payloads[start:end]
            subset_params = params_matrix[start:end]
            if not subset_params:
                start = end
                continue
            cursor = connection.executemany(sql, [tuple(param) for param in subset_params])
            connection.commit()
            generated_start: int | None = None
            if len(pk_columns) == 1 and pk_columns[0] in auto_increment:
                last_rowid_result = connection.execute("SELECT last_insert_rowid()").fetchone()
                if last_rowid_result is None or last_rowid_result[0] is None:
                    raise RuntimeError("Unable to determine lastrowid for bulk insert")
                last_id = int(last_rowid_result[0])
                generated_start = last_id - len(subset_payloads) + 1

            for offset, payload in enumerate(subset_payloads):
                pk_filter: dict[str, object] = {}
                for pk in pk_columns:
                    value = payload.get(pk)
                    if value is None and generated_start is not None and pk == pk_columns[0]:
                        value = generated_start + offset
                        payload[pk] = value
                    if value is None:
                        raise ValueError(f"Primary key column '{pk}' is null after insert")
                    pk_filter[pk] = value
                instance = self._fetch_single(table, pk_filter, include=None)
                self._invalidate_backrefs(table, instance)
                results.append(instance)
            start = end
        return results

    def _resolve_primary_key(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        payload: Mapping[str, object],
        cursor: Any,
    ) -> dict[str, object]:
        primary_key = getattr(table, "primary_key", ())
        if not primary_key:
            raise ValueError(f"Table {table.model.__name__} does not define primary key")
        auto_increment = set(getattr(table, "auto_increment_columns", ()))
        mutable_payload = cast(dict[str, object], payload)
        pk_filter: dict[str, object] = {}
        for pk in primary_key:
            value = mutable_payload.get(pk)
            if value is None and pk in auto_increment:
                value = cursor.lastrowid
                mutable_payload[pk] = value
            if value is None:
                raise ValueError(f"Primary key column '{pk}' is null after insert")
            pk_filter[pk] = value
        return pk_filter

    def _fetch_all(self, sql: str, params: Sequence[Any]) -> list[sqlite3.Row]:
        cursor = self._execute(sql, params)
        return cursor.fetchall()

    def _execute(self, sql: str, params: Sequence[Any], *, auto_commit: bool = True) -> sqlite3.Cursor:
        connection = self._acquire_connection()
        cursor = connection.execute(sql, tuple(params))
        if auto_commit:
            connection.commit()
        return cursor

    def _acquire_connection(self) -> sqlite3.Connection:
        if self._factory is None:
            assert self._connection is not None
            self._ensure_row_factory(self._connection)
            return self._connection

        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = self._factory()
            if not isinstance(connection, sqlite3.Connection):
                raise TypeError("SQLite backend factory must return sqlite3.Connection")
            self._ensure_row_factory(connection)
            self._local.connection = connection
        return connection

    @staticmethod
    def _ensure_row_factory(connection: sqlite3.Connection) -> None:
        if connection.row_factory is None:
            connection.row_factory = sqlite3.Row

    def close(self) -> None:
        if self._factory is None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._clear_identity_map()
            return
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            delattr(self._local, "connection")
        self._clear_identity_map()


def create_backend(provider: str, connection: Any) -> BackendProtocol[Any, Any, Mapping[str, object]]:
    if isinstance(connection, SQLiteBackend):
        return connection
    if provider == "sqlite":
        return SQLiteBackend(connection)
    raise ValueError(f"Unsupported provider '{provider}'")
