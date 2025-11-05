from __future__ import annotations

import sqlite3
import threading
from typing import Any, Mapping, Sequence

from pypika.dialects import SQLLiteQuery
from pypika.queries import QueryBuilder

from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, WhereT

from .base import BackendBase
from .protocols import ConnectionFactory, TableProtocol


class SQLiteBackend(BackendBase):
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
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]:
        items = list(data)
        if not items:
            return []

        payloads = [self._normalize_insert_payload(table, item) for item in items]
        column_names = [spec.name for spec in table.column_specs if spec.name in payloads[0]]
        if not column_names:
            raise ValueError("Insert payload cannot be empty")

        sql_table = self.table_cls(table.model.__name__)
        results: list[ModelT] = []
        step = batch_size if batch_size and batch_size > 0 else len(payloads)
        start = 0
        connection = self._acquire_connection()
        while start < len(payloads):
            end = min(start + step, len(payloads))
            subset_payloads = payloads[start:end]
            if not subset_payloads:
                break
            insert_query: QueryBuilder = self.query_cls.into(sql_table).columns(*column_names)
            params: list[Any] = []
            for payload in subset_payloads:
                insert_query = insert_query.insert(*(self._new_parameter() for _ in column_names))
                params.extend(payload.get(column) for column in column_names)
            sql = self._render_query(insert_query)
            sql_with_returning = self._append_returning(sql, [spec.name for spec in table.column_specs])
            cursor = connection.execute(sql_with_returning, tuple(params))
            try:
                rows = cursor.fetchall()
            finally:
                cursor.close()
            if len(rows) != len(subset_payloads):
                raise RuntimeError("Inserted rows mismatch returning rows")
            connection.commit()
            include_map: Mapping[str, bool] = {}
            for row in rows:
                instance = self._row_to_model(table, row, include_map)
                self._invalidate_backrefs(table, instance)
                results.append(instance)
            start = end
        return results
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
