from __future__ import annotations

from abc import ABC, abstractmethod
import warnings
from typing import Any, Literal, Mapping, Sequence, cast, overload

from pypika import Query, Table
from pypika.enums import Order
from pypika.functions import Count
from pypika.queries import QueryBuilder
from pypika.terms import Criterion, Parameter
from pypika.utils import format_quotes

from dclassql.runtime.sql_recorder import push_sql
from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, OrderDirection, UpsertWhereT, WhereT

from .lazy import LazyRelationState
from .protocols import BackendProtocol, TableProtocol
from .where_compiler import WhereCompiler


class BackendBase(BackendProtocol, ABC):
    quote_char: str = '"'
    parameter_token: str = '?'
    query_cls: type[Query] = Query
    table_cls: type[Table] = Table
    parameter_cls: type[Parameter] = Parameter
    like_escape_char: str | None = "\\"

    def __init__(self, *, echo_sql: bool = False) -> None:
        self._echo_sql = echo_sql

    def _execute_returning_transaction(
        self,
        sql: str,
        params: Sequence[object],
        *,
        allow_empty: bool,
    ) -> list[dict[str, object]]:
        self._begin_transaction()
        try:
            rows = list(self.query_raw(sql, params, auto_commit=False))
            if len(rows) == 1 or (allow_empty and not rows):
                self._commit_transaction()
            else:
                self._rollback_transaction()
            return rows
        except BaseException:
            self._rollback_transaction()
            raise

    def insert(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: InsertT | ModelT | Mapping[str, object],
    ) -> ModelT:
        payload = table.serialize_insert(data)
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
        result = self._materialize_instance(table, row, include_map={})
        return result

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        data: Sequence[InsertT | ModelT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]:
        _ = batch_size  # 基础实现不做批量优化
        return [self.insert(table, item) for item in data]

    def update(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT,
        include: Mapping[str, bool] | None = None,
    ) -> ModelT:
        payload = table.serialize_update(data)
        if not payload:
            raise ValueError("Update payload cannot be empty")

        sql_table = self.table_cls(table.model.__name__)
        update_query: QueryBuilder = self.query_cls.update(sql_table)
        params: list[Any] = []
        for column, value in payload.items():
            update_query = update_query.set(sql_table.field(column), self.new_parameter())
            params.append(value)

        criterion, where_params = self._compile_where(table, sql_table, where)
        if criterion is not None:
            update_query = update_query.where(criterion)
            params.extend(where_params)

        sql = self._render_query(update_query)
        returning_columns = [spec.name for spec in table.column_specs]
        sql_with_returning = self._append_returning(sql, returning_columns)

        rows = self._execute_returning_transaction(
            sql_with_returning,
            params,
            allow_empty=False,
        )
        if len(rows) != 1:
            raise RuntimeError(f"update() expected exactly 1 row, got {len(rows)}")

        row = rows[0]
        include_map = include or {}
        instance = self._materialize_instance(table, row, include_map)
        return instance

    def upsert(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: UpsertWhereT,
        update: Mapping[str, object],
        insert: InsertT | ModelT | Mapping[str, object],
        include: Mapping[str, bool] | None = None,
    ) -> ModelT:
        where_payload = dict(where)
        conflict_targets: list[tuple[str, ...]] = []
        if table.primary_key:
            conflict_targets.append(tuple(table.primary_key))
        conflict_targets.extend(tuple(idx) for idx in getattr(table, "unique_indexes", ()))
        if not conflict_targets:
            raise ValueError("upsert requires primary key or unique index")

        where_keys = set(where_payload.keys())
        conflict_target: tuple[str, ...] | None = None
        for target in conflict_targets:
            if set(target) == where_keys:
                conflict_target = target
                break
        if conflict_target is None:
            raise ValueError("Upsert where must exactly match primary key or unique index")

        insert_payload = table.serialize_insert(insert)
        for column in conflict_target:
            if column not in insert_payload:
                insert_payload[column] = where_payload[column]
        if not insert_payload:
            raise ValueError("Upsert insert payload cannot be empty")

        update_payload = table.serialize_update(update)
        if not update_payload:
            raise ValueError("Upsert update payload cannot be empty")

        sql_table = self.table_cls(table.model.__name__)
        insert_columns = list(insert_payload.keys())
        params: list[Any] = [insert_payload[column] for column in insert_columns]

        insert_query: QueryBuilder = (
            self.query_cls.into(sql_table)
            .columns(*insert_columns)
            .insert(*(self.new_parameter() for _ in insert_columns))
        )
        sql_base = self._render_query(insert_query).rstrip().removesuffix(";")

        conflict_target_sql = ", ".join(self.escape_identifier(col) for col in conflict_target)
        update_assignments: list[str] = []
        for column, value in update_payload.items():
            update_assignments.append(f"{self.escape_identifier(column)} = {self.parameter_token}")
            params.append(value)
        update_clause = ", ".join(update_assignments)

        sql = f"{sql_base} ON CONFLICT ({conflict_target_sql}) DO UPDATE SET {update_clause}"
        returning_columns = [spec.name for spec in table.column_specs]
        sql_with_returning = self._append_returning(sql, returning_columns)
        rows = self.query_raw(sql_with_returning, params, auto_commit=True)
        if len(rows) != 1:
            raise RuntimeError(f"upsert() expected exactly 1 row, got {len(rows)}")

        row = rows[0]
        include_map = include or {}
        instance = self._materialize_instance(table, row, include_map)
        return instance

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: OrderByT | None = None,
        distinct: Sequence[str] | str | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]:
        sql_table = self.table_cls(table.model.__name__)
        distinct_columns = self._normalize_distinct(table, distinct)
        select_query, params = self._build_select_query(table, sql_table, where, order_by)

        if skip is not None and not distinct_columns:
            select_query = select_query.offset(skip)
        if take is not None and not distinct_columns:
            select_query = select_query.limit(take)

        sql = self._render_query(select_query)
        rows = self.query_raw(sql, params)
        row_list = list(rows)
        if distinct_columns:
            row_list = self._deduplicate_rows(row_list, distinct_columns)
            if skip:
                row_list = row_list[skip:]
            if take is not None:
                row_list = row_list[:take]
        include_map = include or {}
        return [self._materialize_instance(table, row, include_map) for row in row_list]

    def _build_select_query(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        sql_table: Table,
        where: WhereT | None,
        order_by: OrderByT | None,
    ) -> tuple[QueryBuilder, list[Any]]:
        select_query: QueryBuilder = self.query_cls.from_(sql_table).select(
            *[sql_table.field(spec.name) for spec in table.column_specs]
        )
        params: list[Any] = []

        if where:
            criterion, where_params = self._compile_where(table, sql_table, where)
            if criterion is not None:
                select_query = select_query.where(criterion)
                params.extend(where_params)

        for column, order in self._normalize_order_by(table, order_by):
            select_query = select_query.orderby(sql_table.field(column), order=order)

        return select_query, params

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: OrderByT | None = None,
        distinct: Sequence[str] | str | None = None,
        skip: int | None = None,
    ) -> ModelT | None:
        results = self.find_many(
            table,
            where=where,
            include=include,
            order_by=order_by,
            distinct=distinct,
            take=1,
            skip=skip,
        )
        return results[0] if results else None

    def count(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
    ) -> int:
        sql_table = self.table_cls(table.model.__name__)
        query: QueryBuilder = self.query_cls.from_(sql_table).select(
            Count("*").as_("__count")
        )
        params: list[object] = []
        if where:
            criterion, where_params = self._compile_where(table, sql_table, where)
            if criterion is not None:
                query = query.where(criterion)
                params.extend(where_params)
        rows = self.query_raw(self._render_query(query), params)
        if len(rows) != 1:
            raise RuntimeError(f"count() expected exactly 1 row, got {len(rows)}")
        value = rows[0]["__count"]
        if not isinstance(value, int):
            raise TypeError(f"count() returned a non-integer value: {value!r}")
        return value

    def delete(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT,
        include: Mapping[str, bool] | None = None,
    ) -> ModelT | None:
        sql_table = self.table_cls(table.model.__name__)
        delete_query: QueryBuilder = self.query_cls.from_(sql_table).delete()
        criterion, params = self._compile_where(table, sql_table, where)
        if criterion is None:
            raise ValueError("delete() requires a where clause, if you want to delete more rows use delete_many()")
        delete_query = delete_query.where(criterion)
        sql = self._render_query(delete_query)
        returning_columns = [spec.name for spec in table.column_specs]
        sql_with_returning = self._append_returning(sql, returning_columns)
        rows = self._execute_returning_transaction(
            sql_with_returning,
            params,
            allow_empty=True,
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError(f"delete() expected exactly 1 row, got {len(rows)}")

        row = rows[0]
        include_map = include or {}
        instance = self._materialize_instance(table, row, include_map)
        return instance

    @overload
    def delete_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        return_records: Literal[False] = False,
    ) -> int: ...

    @overload
    def delete_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        return_records: Literal[True],
    ) -> list[ModelT]: ...

    def delete_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        where: WhereT | None = None,
        return_records: Literal[False, True] = False,
    ) -> int | list[ModelT]:
        sql_table = self.table_cls(table.model.__name__)
        delete_query: QueryBuilder = self.query_cls.from_(sql_table).delete()
        params: list[Any] = []

        if where:
            criterion, where_params = self._compile_where(table, sql_table, where)
            if criterion is not None:
                delete_query = delete_query.where(criterion)
                params.extend(where_params)

        sql = self._render_query(delete_query)

        if return_records:
            returning_columns = [spec.name for spec in table.column_specs]
            sql_with_returning = self._append_returning(sql, returning_columns)
            rows = self.query_raw(sql_with_returning, params, auto_commit=True)
            include_map: Mapping[str, bool] = {}
            return [self._materialize_instance(table, row, include_map) for row in rows]

        affected = self.execute_raw(sql, params, auto_commit=True)
        return affected

    @overload
    def update_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT | None = None,
        return_records: Literal[False] = False,
    ) -> int: ...

    @overload
    def update_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT | None = None,
        return_records: Literal[True],
    ) -> list[ModelT]: ...

    def update_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        *,
        data: Mapping[str, object],
        where: WhereT | None = None,
        return_records: Literal[False, True] = False,
    ) -> int | list[ModelT]:
        payload = table.serialize_update(data)
        if not payload:
            raise ValueError("Update payload cannot be empty")

        sql_table = self.table_cls(table.model.__name__)
        update_query: QueryBuilder = self.query_cls.update(sql_table)
        params: list[Any] = []
        for column, value in payload.items():
            update_query = update_query.set(sql_table.field(column), self.new_parameter())
            params.append(value)

        if where:
            criterion, where_params = self._compile_where(table, sql_table, where)
            if criterion is not None:
                update_query = update_query.where(criterion)
                params.extend(where_params)

        sql = self._render_query(update_query)

        if return_records:
            returning_columns = [spec.name for spec in table.column_specs]
            sql_with_returning = self._append_returning(sql, returning_columns)
            rows = self.query_raw(sql_with_returning, params, auto_commit=True)
            include_map: Mapping[str, bool] = {}
            return [self._materialize_instance(table, row, include_map) for row in rows]

        affected = self.execute_raw(sql, params, auto_commit=True)
        return affected

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

    @abstractmethod
    def _begin_transaction(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _commit_transaction(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _rollback_transaction(self) -> None:
        raise NotImplementedError

    def query_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = False) -> Sequence[dict[str, object]]:
        raise NotImplementedError

    def execute_raw(self, sql: str, params: Sequence[object] | None = None, auto_commit: bool = True) -> int:
        raise NotImplementedError

    def escape_identifier(self, name: str) -> str:
        if self.quote_char:
            return format_quotes(name, self.quote_char)
        raise ValueError("Backend does not support identifier quoting without a quote character set")

    def _materialize_instance(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        row: Mapping[str, Any],
        include_map: Mapping[str, bool],
    ) -> ModelT:
        instance = table.deserialize_row(row)
        instance = cast(ModelT, instance)
        self._attach_relations(table, instance, include_map)
        return instance

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

        for relation in relations:
            name = relation.attribute
            state = LazyRelationState(
                attribute=name,
                backend=self,
                table_cls=relation.remote_table(),
                mapping=relation.mapping,
                many=relation.many,
            )
            if include_lookup.get(name):
                state.materialize(instance)
            else:
                state.bind(instance)

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
        params_seq = list(params) if params is not None else []
        push_sql(sql, params_seq, echo=self._echo_sql)

    def _normalize_distinct(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        distinct: Sequence[str] | str | None,
    ) -> tuple[str, ...]:
        if distinct is None:
            return ()
        if isinstance(distinct, str):
            columns = (distinct,)
        else:
            columns = tuple(dict.fromkeys(distinct))
        if not columns:
            raise ValueError("distinct requires at least one column")
        valid_columns = table.column_specs_by_name
        for column in columns:
            if column not in valid_columns:
                raise KeyError(f"Unknown column '{column}' in distinct clause")
        return columns

    def _normalize_order_by(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        order_by: Mapping[str, OrderDirection] | None,
    ) -> tuple[tuple[str, Order], ...]:
        if not order_by:
            return ()
        result: list[tuple[str, Order]] = []
        for column, direction in order_by.items():
            if direction not in Order.__members__:
                raise ValueError(f'{direction=} is not in [desc, asc]')

            result.append((column, Order[direction]))
        return tuple(result)

    def _deduplicate_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        columns: tuple[str, ...],
    ) -> list[Mapping[str, Any]]:
        seen: set[tuple[Any, ...]] = set()
        result: list[Mapping[str, Any]] = []
        for row in rows:
            key = tuple(row[column] for column in columns)
            if key in seen:
                continue
            seen.add(key)
            result.append(row)
        return result
