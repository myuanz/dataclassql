from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import MISSING, fields, is_dataclass
from typing import Any, Mapping, Sequence, cast, get_origin

from pypika import Query, Table
from pypika.enums import Order
from pypika.terms import Parameter

from .lazy import ensure_lazy_state, finalize_lazy_state, reset_lazy_backref
from .protocols import BackendProtocol, RelationSpec, TableProtocol


class BackendBase[ModelT, InsertT, WhereT: Mapping[str, object]](BackendProtocol[ModelT, InsertT, WhereT], ABC):
    quote_char: str = '"'
    parameter_token: str = '?'
    query_cls: type[Query] = Query
    table_cls: type[Table] = Table
    parameter_cls: type[Parameter] = Parameter

    def __init__(self) -> None:
        self._identity_map: dict[tuple[type[Any], tuple[Any, ...]], Any] = {}

    def insert(self, table: TableProtocol[ModelT, InsertT, WhereT], data: InsertT | Mapping[str, object]) -> ModelT:
        payload = self._normalize_insert_payload(table, data)
        if not payload:
            raise ValueError("Insert payload cannot be empty")

        sql_table = self.table_cls(table.model.__name__)
        columns = list(payload.keys())
        params = [payload[name] for name in columns]

        insert_query = (
            self.query_cls.into(sql_table)
            .columns(*columns)
            .insert(*(self._new_parameter() for _ in columns))
        )
        sql = self._render_query(insert_query)
        cursor = self._execute(sql, params)
        pk_filter = self._resolve_primary_key(table, payload, cursor)
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
        _ = batch_size  # 基础实现不做批量优化
        return [self.insert(table, item) for item in data]

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
        sql_table = self.table_cls(table.model.__name__)
        select_query = self.query_cls.from_(sql_table).select(*[sql_table.field(col) for col in table.columns])
        params: list[Any] = []

        if where:
            for column, value in where.items():
                if column not in table.columns:
                    raise KeyError(f"Unknown column '{column}' in where clause")
                field = sql_table.field(column)
                if value is None:
                    select_query = select_query.where(field.isnull())
                else:
                    select_query = select_query.where(field == self._new_parameter())
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

        sql = self._render_query(select_query)
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

    def _row_to_model(
        self,
        table: TableProtocol[ModelT, Any, Any],
        row: Any,
        include_map: Mapping[str, bool],
    ) -> ModelT:
        key = self._identity_key(table, row)
        cached = self._identity_map.get(key) if key is not None else None
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
        row: Any,
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
                        name=entry["name"],
                        table_name=entry["table_name"],
                        table_module=entry.get("table_module", table.__class__.__module__),
                        many=bool(entry["many"]),
                        mapping=tuple(tuple(pair) for pair in entry["mapping"]),
                    )
                )
            else:
                raise TypeError("Unsupported relation specification")

        existing_names: set[str] = {spec.name for spec in relations}
        relations.extend(self._find_backref_relations(table, existing_names))
        if not relations:
            return

        for spec in relations:
            name = spec.name
            table_module_name = spec.table_module or table.__class__.__module__
            module = sys.modules.get(table_module_name)
            if module is None:
                raise RuntimeError(f"Module '{table_module_name}' not loaded for relation '{name}'")
            table_cls_name = spec.table_name
            table_cls = getattr(module, table_cls_name)
            state = ensure_lazy_state(
                instance=instance,
                attribute=name,
                backend=cast(BackendProtocol[Any, Any, Mapping[str, object]], self),
                table_cls=cast(type[Any], table_cls),
                mapping=spec.mapping,
                many=spec.many,
            )
            finalize_lazy_state(instance, state, eager=bool(include_lookup.get(name)))

    def _find_backref_relations(
        self,
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

    def _clear_identity_map(self) -> None:
        self._identity_map.clear()

    def _render_query(self, query: Query) -> str:
        return cast(Any, query).get_sql(quote_char=self.quote_char) + ';'

    def _new_parameter(self) -> Parameter:
        return self.parameter_cls(self.parameter_token)

    def _resolve_primary_key(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        payload: Mapping[str, object],
        cursor: Any,
    ) -> dict[str, object]:
        _ = cursor
        pk_filter: dict[str, object] = {}
        primary_key = getattr(table, "primary_key", ())
        if not primary_key:
            raise ValueError(f"Table {table.model.__name__} does not define primary key")
        for pk in primary_key:
            value = payload.get(pk)
            if value is None:
                raise ValueError(f"Primary key column '{pk}' is null after insert")
            pk_filter[pk] = value
        return pk_filter

    @abstractmethod
    def _fetch_all(self, sql: str, params: Sequence[Any]) -> Sequence[Any]:
        raise NotImplementedError

    @abstractmethod
    def _execute(self, sql: str, params: Sequence[Any], *, auto_commit: bool = True) -> Any:
        raise NotImplementedError
