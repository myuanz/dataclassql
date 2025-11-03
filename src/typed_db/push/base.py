from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from pypika import Query, Table
from pypika.queries import Column, CreateQueryBuilder
from pypika.terms import Index as PypikaIndex

from ..model_inspector import ColumnInfo, ModelInfo
from ..table_spec import TableInfo


@dataclass(slots=True, frozen=True)
class IndexDefinition:
    name: str
    columns: tuple[str, ...]
    unique: bool


class SchemaBuilder(ABC):
    quote_char: str = '"'

    def __init__(self, info: ModelInfo) -> None:
        self.info = info
        self.table_info = TableInfo.from_dc(info.model)

    def build(self) -> tuple[str, list[IndexDefinition]]:
        create_sql = self.render_create_table_sql()
        index_definitions = self.render_index_definitions()
        return create_sql, index_definitions

    def render_create_table_sql(self) -> str:
        builder: CreateQueryBuilder = Query.create_table(self.table_name).if_not_exists()

        pk_cols = self._normalize_col_names(self.table_info.primary_key.col_name())
        pk_set = set(pk_cols)
        single_inline_pk = self._has_inline_primary_key(pk_cols)

        for column in self.info.columns:
            column_sql = self.render_column_definition(
                column=column,
                pk_columns=pk_cols,
                pk_members=pk_set,
                single_inline_pk=single_inline_pk,
            )
            builder = builder.columns(self.make_column(column.name, column_sql))

        seen_unique: set[tuple[str, ...]] = set()
        for spec in self.table_info.unique_index:
            columns = self._normalize_col_names(spec.col_name())
            if columns in seen_unique:
                continue
            seen_unique.add(columns)
            builder = builder.unique(*columns)

        if pk_cols:
            if len(pk_cols) == 1 and not single_inline_pk:
                builder = builder.primary_key(*pk_cols)
            elif len(pk_cols) > 1:
                builder = builder.primary_key(*pk_cols)

        return builder.get_sql(quote_char=self.quote_char) + ";"

    def render_index_definitions(self) -> list[IndexDefinition]:
        definitions: list[IndexDefinition] = []
        seen_unique: set[tuple[str, ...]] = set()
        for spec in self.table_info.index:
            columns = self._normalize_col_names(spec.col_name())
            unique = spec.is_unique_index
            if unique:
                if columns in seen_unique:
                    continue
                seen_unique.add(columns)
            definitions.append(
                IndexDefinition(
                    name=self.make_index_name(columns, unique=unique),
                    columns=columns,
                    unique=unique,
                )
            )
        return definitions

    def make_column(self, name: str, definition: str) -> Column:
        return Column(name, definition)

    def render_column_definition(
        self,
        *,
        column: ColumnInfo,
        pk_columns: tuple[str, ...],
        pk_members: set[str],
        single_inline_pk: bool,
    ) -> str:
        sql_type = self.resolve_column_type(column.python_type)
        if self.use_inline_primary_key(
            column=column,
            pk_columns=pk_columns,
            sql_type=sql_type,
        ):
            return self.inline_primary_key_definition(sql_type)

        column_def = sql_type
        if self.include_not_null(
            column,
            pk_members=pk_members,
            single_inline_pk=single_inline_pk,
        ):
            column_def = self.append_not_null(column_def)
        return column_def

    def include_not_null(
        self,
        column: ColumnInfo,
        *,
        pk_members: set[str],
        single_inline_pk: bool,
    ) -> bool:
        if column.name in pk_members and single_inline_pk and column.auto_increment:
            return False
        if column.name in pk_members:
            return False
        return not column.optional

    def make_index_name(self, columns: tuple[str, ...], *, unique: bool) -> str:
        suffix = "_".join(columns)
        prefix = "uq" if unique else "idx"
        return f"{prefix}_{self.table_name}_{suffix}"

    def create_index_sql(self, definition: IndexDefinition) -> str:
        table = Table(self.table_name)
        index = PypikaIndex(definition.name)
        columns_sql = ", ".join(
            table.field(column).get_sql(quote_char=self.quote_char) for column in definition.columns
        )
        unique_keyword = "UNIQUE " if definition.unique else ""
        index_sql = (
            f"CREATE {unique_keyword}INDEX IF NOT EXISTS "
            f"{index.get_sql(quote_char=self.quote_char)} "
            f"ON {table.get_sql(quote_char=self.quote_char)} ({columns_sql});"
        )
        return index_sql

    def drop_index_sql(self, index_name: str) -> str:
        index = PypikaIndex(index_name)
        return f"DROP INDEX IF EXISTS {index.get_sql(quote_char=self.quote_char)};"

    def _has_inline_primary_key(self, pk_columns: tuple[str, ...]) -> bool:
        if len(pk_columns) != 1:
            return False
        pk_name = pk_columns[0]
        for column in self.info.columns:
            if column.name != pk_name:
                continue
            sql_type = self.resolve_column_type(column.python_type)
            return self.use_inline_primary_key(
                column=column,
                pk_columns=pk_columns,
                sql_type=sql_type,
            )
        return False

    def _normalize_col_names(self, spec_cols: Any) -> tuple[str, ...]:
        if isinstance(spec_cols, tuple):
            return tuple(spec_cols)
        if isinstance(spec_cols, list):
            return tuple(spec_cols)
        return (spec_cols,)

    @property
    def table_name(self) -> str:
        return self.info.model.__name__

    @abstractmethod
    def resolve_column_type(self, annotation: Any) -> str:
        ...

    def use_inline_primary_key(
        self,
        *,
        column: ColumnInfo,
        pk_columns: tuple[str, ...],
        sql_type: str,
    ) -> bool:
        if len(pk_columns) != 1:
            return False
        return False

    def inline_primary_key_definition(self, sql_type: str) -> str:
        raise NotImplementedError("Inline primary key definition not supported for this backend")

    def append_not_null(self, definition: str) -> str:
        return f"{definition} NOT NULL"


class DatabasePusher(ABC):
    schema_builder_cls: type[SchemaBuilder]

    @abstractmethod
    def fetch_existing_indexes(self, conn: Any, info: ModelInfo) -> set[str]:
        ...

    @abstractmethod
    def execute_statements(self, conn: Any, statements: Iterable[str]) -> None:
        ...

    def is_system_index(self, name: str) -> bool:
        return False

    def validate_connection(self, conn: Any) -> None:
        return None

    def push(self, conn: Any, infos: Sequence[ModelInfo], *, sync_indexes: bool = False) -> None:
        self.validate_connection(conn)
        for info in infos:
            builder = self.schema_builder_cls(info)
            create_sql, index_definitions = builder.build()
            statements: list[str] = [create_sql]

            existing_indexes = self.fetch_existing_indexes(conn, info)
            expected_names = {definition.name for definition in index_definitions}

            if sync_indexes:
                for index_name in sorted(existing_indexes):
                    if self.is_system_index(index_name):
                        continue
                    if index_name in expected_names:
                        continue
                    statements.append(builder.drop_index_sql(index_name))

            for definition in index_definitions:
                if definition.name in existing_indexes:
                    continue
                statements.append(builder.create_index_sql(definition))

            self.execute_statements(conn, statements)
