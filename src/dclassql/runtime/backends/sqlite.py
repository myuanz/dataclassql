from __future__ import annotations

import sqlite3
import sys
import threading
from dataclasses import MISSING, fields, is_dataclass
from typing import Any, Mapping, Sequence, cast, get_origin

from pypika import Query, Table
from pypika.enums import Order
from pypika.terms import Parameter
from .lazy import ensure_lazy_state, finalize_lazy_state, reset_lazy_backref
from .protocols import BackendProtocol, ConnectionFactory, RelationSpec, TableProtocol


def _find_backref_relations(
    table: TableProtocol[Any, Any, Any],
    existing_names: set[str],
) -> list[RelationSpec]:
    model = table.model
    module_name = table.__class__.__module__
    module = sys.modules.get(module_name)
    if module is None:
        return []

    extras: list[RelationSpec] = []
    for attr in dir(module):
        candidate = getattr(module, attr, None)
        if not isinstance(candidate, type):
            continue
        if candidate is table.__class__:
            continue
        foreign_keys = getattr(candidate, "foreign_keys", None)
        if not foreign_keys:
            continue
        candidate_model = getattr(candidate, "model", None)
        if candidate_model is None:
            continue
        for fk in foreign_keys:
            if fk.backref is None:
                continue
            if fk.remote_model is not model:
                continue
            name = fk.backref
            if name in existing_names:
                continue
            mapping_pairs: tuple[tuple[str, str], ...] = tuple(
                (remote, local) for remote, local in zip(fk.remote_columns, fk.local_columns)
            )
            primary_key = getattr(candidate, "primary_key", ())
            many = tuple(primary_key) != tuple(fk.local_columns)
            extras.append(
                RelationSpec(
                    name=name,
                    table_name=candidate.__name__,
                    table_module=candidate.__module__,
                    many=many,
                    mapping=mapping_pairs,
                )
            )
            existing_names.add(name)
    return extras


class SQLiteBackend[ModelT, InsertT, WhereT: Mapping[str, object]](BackendProtocol[ModelT, InsertT, WhereT]):
    def __init__(self, source: sqlite3.Connection | ConnectionFactory | "SQLiteBackend") -> None:
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
            self._identity_map: dict[tuple[type[Any], tuple[Any, ...]], Any] = {}
        elif callable(source):
            self._factory = source
            self._connection = None
            self._local = threading.local()
            self._identity_map = {}
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

        result = self._fetch_single(table, pk_filter, include=None)
        self._invalidate_backrefs(table, result)
        return result

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
                instance = self._fetch_single(table, pk_filter, include=None)
                self._invalidate_backrefs(table, instance)
                results.append(instance)
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
        include_map = include or {}
        return [self._row_to_model(table, row, include_map) for row in rows]

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

    def _fetch_single(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        where: Mapping[str, object],
        include: Mapping[str, bool] | None,
    ) -> ModelT:
        results = self.find_many(table, where=cast(WhereT, where), include=include)
        if not results:
            raise RuntimeError("Inserted row could not be reloaded")
        return results[0]

    def _normalize_insert_payload(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: InsertT | Mapping[str, object],
    ) -> dict[str, object]:
        allowed = set(table.columns)
        if isinstance(data, Mapping):
            return {key: data[key] for key in allowed if key in data}
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

    def _row_to_model(
        self,
        table: TableProtocol[ModelT, Any, Any],
        row: sqlite3.Row,
        include_map: Mapping[str, bool],
    ) -> ModelT:
        key = self._identity_key(table, row)
        if key is not None:
            cached = self._identity_map.get(key)
        else:
            cached = None
        model = table.model
        if cached is None:
            if is_dataclass(model):
                values: dict[str, Any] = {column: row[column] for column in table.columns}
                instance = model.__new__(model)
                for field in fields(model):
                    if field.name in values:
                        value = values[field.name]
                    elif field.default is not MISSING:
                        value = field.default
                    elif field.default_factory is not MISSING:  # type: ignore[attr-defined]
                        value = field.default_factory()  # type: ignore[misc]
                    else:
                        origin = get_origin(field.type)
                        if origin in (list, set, frozenset):
                            value = origin()  # type: ignore[call-arg]
                        else:
                            value = None
                    object.__setattr__(instance, field.name, value)
            else:
                instance = cast(ModelT, model(**{column: row[column] for column in table.columns}))
            if key is not None:
                self._identity_map[key] = instance
        else:
            instance = cast(ModelT, cached)
            for column in table.columns:
                object.__setattr__(instance, column, row[column])
        self._attach_relations(table, instance, include_map)
        return instance

    def _invalidate_backrefs(
        self,
        table: TableProtocol[ModelT, Any, Any],
        instance: ModelT,
    ) -> None:
        foreign_keys = getattr(table, "foreign_keys", ())
        if not foreign_keys:
            return
        for fk in foreign_keys:
            backref = getattr(fk, "backref", None)
            if not backref:
                continue
            remote_model = fk.remote_model
            if remote_model is None:
                continue
            key_values: list[Any] = []
            for local_col, remote_col in zip(fk.local_columns, fk.remote_columns):
                value = getattr(instance, local_col, None)
                if value is None:
                    key_values = []
                    break
                key_values.append(value)
            if not key_values:
                continue
            identity_key = (remote_model, tuple(key_values))
            owner = self._identity_map.get(identity_key)
            if owner is None:
                continue
            reset_lazy_backref(owner, backref)

    def _identity_key(
        self,
        table: TableProtocol[ModelT, Any, Any],
        row: sqlite3.Row,
    ) -> tuple[type[Any], tuple[Any, ...]] | None:
        pk_columns = getattr(table, "primary_key", ())
        if not pk_columns:
            return None
        values: list[Any] = []
        for column in pk_columns:
            value = row[column]
            if value is None:
                return None
            values.append(value)
        return (table.model, tuple(values))

    def _attach_relations(
        self,
        table: TableProtocol[ModelT, Any, Any],
        instance: ModelT,
        include_map: Mapping[str, bool],
    ) -> None:
        include_lookup = dict(include_map)
        relations_attr = cast(Sequence[Any], getattr(table, "relations", ()))
        relations: list[RelationSpec] = []
        for entry in relations_attr:
            if isinstance(entry, RelationSpec):
                relations.append(entry)
            elif isinstance(entry, dict):
                relations.append(
                    RelationSpec(
                        name=entry['name'],
                        table_name=entry['table_name'],
                        table_module=entry.get('table_module', table.__class__.__module__),
                        many=bool(entry['many']),
                        mapping=tuple(tuple(pair) for pair in entry['mapping']),
                    )
                )
            else:
                raise TypeError("Unsupported relation specification")

        existing_names: set[str] = {spec.name for spec in relations}
        relations.extend(_find_backref_relations(table, existing_names))
        if not relations:
            return

        backend_for_lazy = cast(BackendProtocol[Any, Any, Mapping[str, object]], self)

        for spec in relations:
            name = spec.name
            table_module_name = spec.table_module or table.__class__.__module__
            module = sys.modules.get(table_module_name)
            if module is None:
                raise RuntimeError(f"Module '{table_module_name}' not loaded for relation '{name}'")
            table_cls_name = spec.table_name
            table_cls = getattr(module, table_cls_name)
            mapping = spec.mapping
            many = spec.many
            state = ensure_lazy_state(
                instance=instance,
                attribute=name,
                backend=backend_for_lazy,
                table_cls=cast(type[Any], table_cls),
                mapping=mapping,
                many=many,
            )
            eager = bool(include_lookup.get(name))
            finalize_lazy_state(instance, state, eager=eager)

    def close(self) -> None:
        if self._factory is None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._identity_map.clear()
            return
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            delattr(self._local, "connection")
        self._identity_map.clear()


def create_backend(provider: str, connection: Any) -> BackendProtocol[Any, Any, Mapping[str, object]]:
    if isinstance(connection, SQLiteBackend):
        return connection
    if provider == "sqlite":
        return SQLiteBackend(connection)
    raise ValueError(f"Unsupported provider '{provider}'")
