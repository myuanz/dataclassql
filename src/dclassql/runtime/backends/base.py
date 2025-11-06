from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import MISSING, fields, is_dataclass
from typing import Any, Literal, Mapping, Sequence, Type, cast, get_origin
from weakref import ReferenceType, ref

from pypika import Query, Table
from pypika.enums import Order
from pypika.terms import Criterion, Parameter
from pypika.queries import QueryBuilder
from pypika.utils import format_quotes

from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, WhereT

from .lazy import ensure_lazy_state, finalize_lazy_state, reset_lazy_backref
from .protocols import BackendProtocol, RelationSpec, TableProtocol
from .where_compiler import WhereCompiler


class BackendBase(BackendProtocol, ABC):
    quote_char: str = '"'
    parameter_token: str = '?'
    query_cls: type[Query] = Query
    table_cls: type[Table] = Table
    parameter_cls: type[Parameter] = Parameter

    def __init__(self, *, echo_sql: bool = False) -> None:
        self._identity_map: dict[tuple[type[Any], tuple[Any, ...]], list[ReferenceType[object]]] = {}
        self._echo_sql = echo_sql

    def insert(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: InsertT | Mapping[str, object],
    ) -> ModelT:
        payload = self._normalize_insert_payload(table, data)
        if not payload:
            raise ValueError("Insert payload cannot be empty")

        sql_table = self.table_cls(table.model.__name__)
        column_names = [spec.name for spec in table.column_specs if spec.name in payload]
        params = [payload[name] for name in column_names]

        insert_query: QueryBuilder = (
            self.query_cls.into(sql_table)
            .columns(*column_names)
            .insert(*(self.new_parameter() for _ in column_names))
        )
        sql = self._render_query(insert_query)
        returning_columns = [spec.name for spec in table.column_specs]
        sql_with_returning = self._append_returning(sql, returning_columns)

        row = self.query_raw(sql_with_returning, params, auto_commit=True)[0]

        result = self._row_to_model(table, row, include_map={})
        self._invalidate_backrefs(table, result)
        return result

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]:
        _ = batch_size  # 基础实现不做批量优化
        return [self.insert(table, item) for item in data]

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Mapping[str, str] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]:
        sql_table = self.table_cls(table.model.__name__)
        select_query = self.query_cls.from_(sql_table).select(
            *[sql_table.field(spec.name) for spec in table.column_specs]
        )
        params: list[Any] = []

        if where:
            criterion, where_params = self._compile_where(table, sql_table, where)
            if criterion is not None:
                select_query = select_query.where(criterion)
                params.extend(where_params)

        if order_by:
            for column, direction in order_by.items():
                if column not in table.column_specs_by_name:
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
        rows = self.query_raw(sql, params)
        include_map = include or {}
        return [self._row_to_model(table, row, include_map) for row in rows]

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Mapping[str, str] | None = None,
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
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: InsertT | Mapping[str, object],
    ) -> dict[str, object]:
        spec_map = table.column_specs_by_name
        if isinstance(data, Mapping):
            return {key: data[key] for key in spec_map.keys() if key in data}
        insert_model = table.insert_model
        if isinstance(data, insert_model):
            return {column: getattr(data, column) for column in spec_map.keys() if hasattr(data, column)}
        if is_dataclass(data):
            return {column: getattr(data, column) for column in spec_map.keys() if hasattr(data, column)}
        raise TypeError("Unsupported insert payload type")

    def _row_to_model(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        row: Any,
        include_map: Mapping[str, bool],
    ) -> ModelT:
        key = self._identity_key(table, row)
        model = table.model
        if is_dataclass(model):
            values: dict[str, Any] = {spec.name: row[spec.name] for spec in table.column_specs}
            instance = model.__new__(model)
            for field in fields(model):
                if field.name in values:
                    value = values[field.name]
                elif field.default is not MISSING:
                    value = field.default
                elif field.default_factory is not MISSING:
                    value = field.default_factory()
                else:
                    origin = get_origin(field.type)
                    if origin in (list, set, frozenset):
                        value = origin()
                    else:
                        value = None
                object.__setattr__(instance, field.name, value)
        else:
            instance = model(**{spec.name: row[spec.name] for spec in table.column_specs})
        if key is not None:
            owners = self._identity_map.get(key)
            alive_refs: list[ReferenceType[object]] = []
            if owners is not None:
                for owner_ref in owners:
                    if owner_ref() is not None:
                        alive_refs.append(owner_ref)
            alive_refs.append(ref(instance))
            self._identity_map[key] = alive_refs
        instance = cast(ModelT, instance)
        self._attach_relations(table, instance, include_map)
        return instance

    def _fetch_single(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        where: Mapping[str, object],
        include: Mapping[str, bool] | None,
    ) -> ModelT:
        results = self.find_many(table, where=cast(WhereT, where), include=include)
        if not results:
            raise RuntimeError("Inserted row could not be reloaded")
        return results[0]

    def query_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = False) -> Sequence[object]:
        raise NotImplementedError

    def execute_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = True) -> int:
        raise NotImplementedError

    def escape_identifier(self, name: str) -> str:
        if self.quote_char:
            return format_quotes(name, self.quote_char)
        raise ValueError("Backend does not support identifier quoting without a quote character set")

    def _invalidate_backrefs(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        instance: ModelT,
    ) -> None:
        foreign_keys = table.foreign_keys
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
            owners = self._identity_map.get(identity_key)
            if not owners:
                continue
            alive_refs: list[ReferenceType[object]] = []
            for owner_ref in owners:
                owner = owner_ref()
                if owner is None:
                    continue
                reset_lazy_backref(owner, backref)
                alive_refs.append(owner_ref)
            if alive_refs:
                self._identity_map[identity_key] = alive_refs
            else:
                self._identity_map.pop(identity_key, None)

    def _identity_key(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        row: Any,
    ) -> tuple[type[ModelT], tuple[Any, ...]] | None:
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
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        instance: ModelT,
        include_map: Mapping[str, bool],
    ) -> None:
        include_lookup = dict(include_map)
        relations = table.relations
        if not relations:
            return

        for spec in relations:
            name = spec.name
            if spec.table_factory is not None:
                table_cls = spec.table_factory()
            else:
                table_module_name = spec.table_module or table.__class__.__module__
                module = sys.modules.get(table_module_name)
                if module is None:
                    raise RuntimeError(f"Module '{table_module_name}' not loaded for relation '{name}'")
                table_cls = getattr(module, spec.table_name)
                table_cls = cast(type[BackendProtocol], table_cls)
            state = ensure_lazy_state(
                instance=instance,
                attribute=name,
                backend=self,
                table_cls=table_cls,
                mapping=spec.mapping,
                many=spec.many,
            )
            finalize_lazy_state(instance, state, eager=bool(include_lookup.get(name)))

    def _clear_identity_map(self) -> None:
        self._identity_map.clear()

    def _render_query(self, query: QueryBuilder) -> str:
        return query.get_sql(quote_char=self.quote_char) + ';'

    def new_parameter(self) -> Parameter:
        return self.parameter_cls(self.parameter_token)

    def _append_returning(self, sql: str, columns: Sequence[str]) -> str:
        trimmed = sql.rstrip()
        if trimmed.endswith(';'):
            trimmed = trimmed[:-1]
        if not columns:
            raise RuntimeError("RETURNING requires at least one column")
        if self.quote_char:
            column_sql = ", ".join(format_quotes(column, self.quote_char) for column in columns)
        else:
            column_sql = ", ".join(columns)
        return f"{trimmed} RETURNING {column_sql};"

    def _compile_where(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        sql_table: Table,
        where: Mapping[str, object],
    ) -> tuple[Criterion | None, list[object]]:
        compiler = WhereCompiler(self, table, sql_table)
        criterion = compiler.compile(where)
        return criterion, compiler.params

    def _log_sql(self, sql: str, params: Sequence[object] | None) -> None:
        if not self._echo_sql:
            return
        if params is None:
            display_params = []
        else:
            display_params = list(params)
        print(f"[dclassql] SQL: {sql} | params={display_params}")
