from __future__ import annotations

import importlib
from collections.abc import Mapping as ABCMapping
from collections.abc import Sequence as ABCSequence
from typing import Literal, Mapping, Protocol, Sequence, TypeAlias

from pypika import Query, Table
from pypika.queries import QueryBuilder
from pypika.terms import Criterion, ExistsCriterion, Field, Parameter

from dclassql.typing import IncludeT, InsertT, ModelT, OrderByT, WhereT

from .protocols import BackendProtocol, RelationSpec, TableProtocol


class WhereBackend(BackendProtocol, Protocol):
    def _new_bound_parameter(self, params: list[object], value: object) -> Parameter: ...


def combine_and(criteria: Sequence[Criterion | None]) -> Criterion | None:
    filtered = [criterion for criterion in criteria if criterion is not None]
    if not filtered:
        return None
    result = filtered[0]
    for criterion in filtered[1:]:
        result = result & criterion
    return result


def combine_or(criteria: Sequence[Criterion | None]) -> Criterion | None:
    filtered = [criterion for criterion in criteria if criterion is not None]
    if not filtered:
        return None
    result = filtered[0]
    for criterion in filtered[1:]:
        result = result | criterion
    return result


class WhereCompiler:
    def __init__(
        self,
        backend: WhereBackend,
        table: TableProtocol[ModelT, InsertT, WhereT, IncludeT, OrderByT],
        sql_table: Table,
    ) -> None:
        self._backend = backend
        self._table = table
        self._sql_table = sql_table
        self.params: list[object] = []
        self._relation_map = {
            relation.name: relation for relation in table.relations
        }

    def compile(self, where: Mapping[str, object]) -> Criterion | None:
        if not isinstance(where, ABCMapping):
            raise TypeError("where clause must be a mapping")
        return self._compile_group(where)

    def _compile_group(self, where: Mapping[str, object]) -> Criterion | None:
        criteria: list[Criterion | None] = []
        for key, value in where.items():
            if key == "AND":
                criteria.append(self._compile_and(value))
                continue
            if key == "OR":
                criteria.append(self._compile_or(value))
                continue
            if key == "NOT":
                criteria.append(self._compile_not(value))
                continue
            criteria.append(self._compile_column(key, value))
        return combine_and(criteria)

    def _compile_and(self, value: object) -> Criterion | None:
        parts = self._collect_subgroups(value, allow_mapping=True, label="AND")
        return combine_and(parts)

    def _compile_or(self, value: object) -> Criterion | None:
        parts = self._collect_subgroups(value, allow_mapping=False, label="OR")
        return combine_or(parts)

    def _compile_not(self, value: object) -> Criterion | None:
        if isinstance(value, ABCMapping):
            compiled = self._compile_group(value)
            return compiled.negate() if compiled is not None else None
        parts = self._collect_subgroups(value, allow_mapping=False, label="NOT")
        combined = combine_and(parts)
        return combined.negate() if combined is not None else None

    def _collect_subgroups(
        self,
        value: object,
        *,
        allow_mapping: bool,
        label: str,
    ) -> list[Criterion | None]:
        groups: list[Criterion | None] = []
        if isinstance(value, ABCMapping):
            if not allow_mapping:
                raise TypeError(f"{label} expects a sequence of filters")
            groups.append(self._compile_group(value))
            return groups
        entries = self._ensure_sequence(value, label=label)
        for idx, entry in enumerate(entries):
            if not isinstance(entry, ABCMapping):
                raise TypeError(f"{label}[{idx}] must be a mapping")
            groups.append(self._compile_group(entry))
        return groups

    def _compile_column(self, column: str, value: object) -> Criterion | None:
        relation = self._relation_map.get(column)
        if relation is not None:
            return self._compile_relation(relation, value)
        if column not in self._table.column_specs_by_name:
            raise KeyError(f"Unknown column '{column}' in where clause")
        field = self._sql_table.field(column)
        return self._compile_value(field, value)

    def _compile_value(self, field: Field, value: object) -> Criterion | None:
        if isinstance(value, ABCMapping):
            return self._compile_filter(field, value)
        return self._compile_direct(field, value)

    def _compile_direct(self, field: Field, value: object) -> Criterion:
        if value is None:
            return field.isnull()
        parameter = self._backend._new_bound_parameter(self.params, value)
        return field == parameter

    def _compile_filter(self, field: Field, filters: Mapping[str, object]) -> Criterion | None:
        criteria: list[Criterion | None] = []
        for key, operand in filters.items():
            if not isinstance(key, str):
                raise TypeError("Filter operator must be a string key")
            criteria.append(self._compile_operator(field, key.upper(), operand))
        return combine_and(criteria)

    def _compile_operator(self, field: Field, operator: str, operand: object) -> Criterion | None:
        if operator == "EQ":
            return self._compile_direct(field, operand)
        if operator == "IN":
            values = self._ensure_sequence(operand, label="IN")
            if not values:
                return (field == field).negate()
            params = tuple(self._backend._new_bound_parameter(self.params, item) for item in values)
            return field.isin(params)
        if operator == "NOT_IN":
            values = self._ensure_sequence(operand, label="NOT_IN")
            if not values:
                return field == field
            params = tuple(self._backend._new_bound_parameter(self.params, item) for item in values)
            return field.notin(params)
        if operator == "LT":
            parameter = self._backend._new_bound_parameter(self.params, operand)
            return field < parameter
        if operator == "LTE":
            parameter = self._backend._new_bound_parameter(self.params, operand)
            return field <= parameter
        if operator == "GT":
            parameter = self._backend._new_bound_parameter(self.params, operand)
            return field > parameter
        if operator == "GTE":
            parameter = self._backend._new_bound_parameter(self.params, operand)
            return field >= parameter
        if operator == "CONTAINS":
            text = self._ensure_string(operand, operator)
            return self._apply_like(field, f"%{text}%")
        if operator == "STARTS_WITH":
            text = self._ensure_string(operand, operator)
            return self._apply_like(field, f"{text}%")
        if operator == "ENDS_WITH":
            text = self._ensure_string(operand, operator)
            return self._apply_like(field, f"%{text}")
        if operator == "NOT":
            if isinstance(operand, ABCMapping):
                compiled = self._compile_filter(field, operand)
            else:
                compiled = self._compile_direct(field, operand)
            return compiled.negate() if compiled is not None else None
        raise ValueError(f"Unsupported filter operator '{operator}'")

    def _ensure_sequence(self, value: object, *, label: str) -> list[object]:
        if isinstance(value, ABCSequence) and not isinstance(value, (str, bytes)):
            return list(value)
        raise TypeError(f"{label} expects a sequence of values")

    def _ensure_string(self, value: object, operator: str) -> str:
        if isinstance(value, str):
            return value
        raise TypeError(f"{operator} expects a string operand")

    def _apply_like(self, field: Field, pattern: str) -> Criterion:
        parameter = self._backend._new_bound_parameter(self.params, pattern)
        # pypika 的 Field.like 允许 Term 参数, 但类型标注仅接受 str
        return field.like(parameter)  # type: ignore[arg-type]

    def _compile_relation(
        self,
        relation: RelationSpec[TableProtocol],
        value: object,
    ) -> Criterion | None:
        if relation.many:
            return self._compile_relation_many(relation, value)
        return self._compile_relation_single(relation, value)

    def _compile_relation_single(
        self,
        relation: RelationSpec[TableProtocol],
        value: object,
    ) -> Criterion | None:
        if not isinstance(value, ABCMapping):
            if value is None:
                return self._relation_is(relation, None)
            raise TypeError("Relation filter expects a mapping")
        criteria: list[Criterion | None] = []
        for raw_key, operand in value.items():
            key = raw_key.upper()
            if key == "IS":
                criteria.append(self._relation_is(relation, operand))
            elif key == "IS_NOT":
                criteria.append(self._relation_is_not(relation, operand))
            else:
                raise ValueError(f"Unsupported relation operator '{raw_key}' for relation '{relation.name}'")
        return combine_and(criteria)

    def _compile_relation_many(
        self,
        relation: RelationSpec[TableProtocol],
        value: object,
    ) -> Criterion | None:
        if not isinstance(value, ABCMapping):
            raise TypeError("Collection relation filter expects a mapping")
        criteria: list[Criterion | None] = []
        for raw_key, operand in value.items():
            key = raw_key.upper()
            if key == "SOME":
                criteria.append(self._relation_exists(relation, operand))
            elif key == "NONE":
                criteria.append(self._relation_exists(relation, operand).negate())
            elif key == "EVERY":
                criteria.append(self._relation_every(relation, operand))
            else:
                raise ValueError(f"Unsupported relation operator '{raw_key}' for relation '{relation.name}'")
        return combine_and(criteria)

    def _relation_is(self, relation: RelationSpec[TableProtocol], operand: object) -> Criterion:
        query, remote_table, remote_instance = self._relation_subquery(relation)
        if operand is None:
            return ExistsCriterion(query).negate()
        if not isinstance(operand, ABCMapping):
            raise TypeError("Relation IS operand must be mapping or None")
        criterion = self._compile_remote_filter(relation, operand, remote_table, remote_instance)
        if criterion is not None:
            query = query.where(criterion)
        return ExistsCriterion(query)

    def _relation_is_not(self, relation: RelationSpec[TableProtocol], operand: object) -> Criterion:
        query, remote_table, remote_instance = self._relation_subquery(relation)
        if operand is None:
            return ExistsCriterion(query)
        if not isinstance(operand, ABCMapping):
            raise TypeError("Relation IS_NOT operand must be mapping or None")
        criterion = self._compile_remote_filter(relation, operand, remote_table, remote_instance)
        if criterion is not None:
            query = query.where(criterion)
        return ExistsCriterion(query).negate()

    def _relation_every(
        self,
        relation: RelationSpec[TableProtocol],
        operand: object,
    ) -> Criterion | None:
        if not isinstance(operand, ABCMapping):
            raise TypeError("EVERY operand must be a mapping")
        query, remote_table, remote_instance = self._relation_subquery(relation)
        criterion = self._compile_remote_filter(relation, operand, remote_table, remote_instance)
        if criterion is None:
            return None
        query = query.where(criterion.negate())
        return ExistsCriterion(query).negate()

    def _relation_exists(self, relation: RelationSpec[TableProtocol], operand: object) -> Criterion:
        query, remote_table, remote_instance = self._relation_subquery(relation)
        if operand is not None:
            if not isinstance(operand, ABCMapping):
                raise TypeError("Relation filter expects mapping or None")
            criterion = self._compile_remote_filter(relation, operand, remote_table, remote_instance)
            if criterion is not None:
                query = query.where(criterion)
        return ExistsCriterion(query)

    def _relation_subquery(
        self,
        relation: RelationSpec[TableProtocol],
    ) -> tuple[QueryBuilder, Table, TableProtocol]:
        table_cls = self._resolve_relation_table_cls(relation)
        remote_instance = table_cls(self._backend)
        remote_table = Table(remote_instance.table_name)
        query = Query.from_(remote_table).select(1)
        for owner_column, target_column in relation.mapping:
            query = query.where(remote_table.field(target_column) == self._sql_table.field(owner_column))
        return query, remote_table, remote_instance

    def _resolve_relation_table_cls(self, relation: RelationSpec[TableProtocol]) -> type[TableProtocol]:
        if relation.table_factory is not None:
            return relation.table_factory()
        module_name = relation.table_module
        if module_name is None:
            raise RuntimeError(f"Cannot resolve table module for relation '{relation.name}'")
        module = importlib.import_module(module_name)
        table_cls = getattr(module, relation.table_name)
        if not isinstance(table_cls, type):
            raise TypeError(f"Relation table '{relation.table_name}' must be a class")
        return table_cls

    def _compile_remote_filter(
        self,
        relation: RelationSpec[TableProtocol],
        operand: Mapping[str, object],
        remote_table: Table,
        remote_instance: TableProtocol,
    ) -> Criterion | None:
        compiler = WhereCompiler(self._backend, remote_instance, remote_table)
        criterion = compiler.compile(operand)
        self.params.extend(compiler.params)
        return criterion
