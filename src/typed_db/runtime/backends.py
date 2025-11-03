from __future__ import annotations

import sqlite3
import threading
from dataclasses import is_dataclass
from typing import (
    Any,
    Callable,
    Mapping,
    Protocol,
    Sequence,
    cast,
    runtime_checkable,
)

from pypika import Query, Table
from pypika.enums import Order
from pypika.terms import Parameter


ConnectionFactory = Callable[[], sqlite3.Connection]


@runtime_checkable
class TableProtocol[ModelT, InsertT, WhereT](Protocol):
    model: type[ModelT]
    insert_model: type[InsertT]
    columns: tuple[str, ...]
    auto_increment_columns: tuple[str, ...]
    primary_key: tuple[str, ...]


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


class SQLiteBackend[ModelT, InsertT, WhereT: Mapping[str, object]](BackendProtocol[ModelT, InsertT, WhereT]):
    def __init__(self, source: sqlite3.Connection | ConnectionFactory | "SQLiteBackend") -> None:
        if isinstance(source, SQLiteBackend):
            self._factory: ConnectionFactory | None = source._factory
            self._connection: sqlite3.Connection | None = source._connection
            self._local = source._local
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

    def insert(self, table: TableProtocol[ModelT, InsertT, WhereT], data: InsertT | Mapping[str, object]) -> ModelT:
        payload = self._normalize_insert_payload(table, data)
        if not payload:
            raise ValueError("Insert payload cannot be empty")

        table_name = table.model.__name__
        sql_table = Table(table_name)
        columns = list(payload.keys())
        params = [payload[name] for name in columns]

        insert_query = (
            Query.into(sql_table)
            .columns(*columns)
            .insert(*(Parameter("?") for _ in columns))
        )
        sql = insert_query.get_sql(quote_char='"') + ";"
        cursor = self._execute(sql, params)

        pk_filter: dict[str, object] = {}
        auto_increment = set(getattr(table, "auto_increment_columns", ()))
        primary_key = getattr(table, "primary_key", ())
        if not primary_key:
            raise ValueError(f"Table {table_name} does not define primary key")
        if len(primary_key) == 1:
            pk = primary_key[0]
            value = payload.get(pk)
            if value is None and pk in auto_increment:
                value = cursor.lastrowid
            if value is None:
                raise ValueError(f"Primary key column '{pk}' is null after insert")
            pk_filter[pk] = value
        else:
            for pk in primary_key:
                value = payload.get(pk)
                if value is None:
                    raise ValueError(f"Composite key requires '{pk}' value")
                pk_filter[pk] = value

        return self._fetch_single(table, pk_filter)

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
        sql_table = Table(table_name)
        insert_query = (
            Query.into(sql_table)
            .columns(*columns)
            .insert(*(Parameter("?") for _ in columns))
        )
        sql = insert_query.get_sql(quote_char='"') + ';'

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
                results.append(self._fetch_single(table, pk_filter))
            start = end
        return results

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]:
        if include:
            raise NotImplementedError("Include is not supported yet")

        table_name = table.model.__name__
        sql_table = Table(table_name)
        select_query = Query.from_(sql_table).select(*[sql_table.field(col) for col in table.columns])
        params: list[Any] = []

        if where:
            for column, value in where.items():
                if column not in table.columns:
                    raise KeyError(f"Unknown column '{column}' in where clause")
                field = sql_table.field(column)
                if value is None:
                    select_query = select_query.where(field.isnull())
                else:
                    select_query = select_query.where(field == Parameter("?"))
                    params.append(value)

        if order_by:
            for column, direction in order_by:
                if column not in table.columns:
                    raise KeyError(f"Unknown column '{column}' in order_by clause")
                direction_lower = direction.lower()
                if direction_lower not in {"asc", "desc"}:
                    raise ValueError("order_by direction must be 'asc' or 'desc'")
                select_query = select_query.orderby(sql_table.field(column), order=Order[direction_lower])

        if skip is not None:
            select_query = select_query.offset(skip)
        if take is not None:
            select_query = select_query.limit(take)

        sql = select_query.get_sql(quote_char='"') + ";"
        rows = self._fetch_all(sql, params)
        return [self._row_to_model(table, row) for row in rows]

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        skip: int | None = None,
    ) -> ModelT | None:
        results = self.find_many(
            table,
            where=where,
            include=include,
            order_by=order_by,
            take=1,
            skip=skip,
        )
        return results[0] if results else None

    def _fetch_single(self, table: TableProtocol[ModelT, InsertT, WhereT], where: Mapping[str, object]) -> ModelT:
        results = self.find_many(table, where=cast(WhereT, where))
        if not results:
            raise RuntimeError("Inserted row could not be reloaded")
        return results[0]

    def _normalize_insert_payload(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: InsertT | Mapping[str, object],
    ) -> dict[str, object]:
        allowed = set(table.columns)
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if k in allowed}
        insert_model = getattr(table, "insert_model", None)
        if insert_model and isinstance(data, insert_model):
            return {column: getattr(data, column) for column in table.columns if hasattr(data, column)}
        if is_dataclass(data):
            return {column: getattr(data, column) for column in table.columns if hasattr(data, column)}
        raise TypeError("Unsupported insert payload type")

    def _fetch_all(self, sql: str, params: Sequence[Any]) -> list[sqlite3.Row]:
        cursor = self._execute(sql, params)
        return cursor.fetchall()

    def _execute(self, sql: str, params: Sequence[Any], auto_commit: bool = True) -> sqlite3.Cursor:
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

    @staticmethod
    def _row_to_model(table: TableProtocol[ModelT, Any, Any], row: sqlite3.Row) -> ModelT:
        data = {column: row[column] for column in table.columns}
        model = table.model
        return cast(ModelT, model(**data))

    def close(self) -> None:
        if self._factory is None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            return
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            delattr(self._local, "connection")


def create_backend(provider: str, connection: Any) -> BackendProtocol[Any, Any, Mapping[str, object]]:
    if isinstance(connection, SQLiteBackend):
        return connection
    if provider == "sqlite":
        return SQLiteBackend(connection)
    raise ValueError(f"Unsupported provider '{provider}'")
