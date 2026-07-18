from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

from pypika import Query, Table
from pypika.queries import Column
from pypika.terms import Index as PypikaIndex

from ..runtime.backends.metadata import ColumnSpec
from ..runtime.backends.protocols import SchemaTableProtocol


@dataclass(slots=True, frozen=True)
class ColumnDeclaration:
    name: str
    type_sql: str
    definition_sql: str
    not_null: bool
    primary_key: bool
    auto_increment: bool


@dataclass(slots=True, frozen=True)
class IndexDefinition:
    name: str
    columns: tuple[str, ...]
    unique: bool


@dataclass(slots=True, frozen=True)
class SchemaPlan:
    create_sql: str
    columns: tuple[ColumnDeclaration, ...]
    indexes: tuple[IndexDefinition, ...]


@dataclass(slots=True, frozen=True)
class ExistingColumn:
    name: str
    type_sql: str
    not_null: bool
    primary_key: bool


@dataclass(slots=True, frozen=True)
class ColumnChange:
    name: str
    current: ExistingColumn
    expected: ColumnDeclaration
    reasons: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class SchemaDiff:
    added: tuple[ColumnDeclaration, ...]
    removed: tuple[ExistingColumn, ...]
    changed: tuple[ColumnChange, ...]

    def is_empty(self) -> bool:
        return not self.added and not self.removed and not self.changed


type ConfirmRebuildCallback = Callable[
    [SchemaTableProtocol, SchemaPlan, tuple[ExistingColumn, ...] | None, SchemaDiff],
    bool,
]


class SchemaBuilder(ABC):
    quote_char: str = '"'

    def __init__(self, table: SchemaTableProtocol) -> None:
        self.table = table
        self._column_declarations: list[ColumnDeclaration] = []
        self._table_name_override: str | None = None

    def build(self, *, table_name: str | None = None) -> SchemaPlan:
        previous = self._table_name_override
        self._table_name_override = table_name
        try:
            create_sql = self.render_create_table_sql()
            index_definitions = self.render_index_definitions()
            return SchemaPlan(
                create_sql=create_sql,
                columns=tuple(self._column_declarations),
                indexes=tuple(index_definitions),
            )
        finally:
            self._table_name_override = previous

    def render_create_table_sql(self) -> str:
        self._column_declarations = []
        builder = Query.create_table(self.table_name).if_not_exists()

        pk_cols = self.table.primary_key
        pk_set = set(pk_cols)
        single_inline_pk = self._has_inline_primary_key(pk_cols)
        column_names = {column.name for column in self.table.column_specs}

        if self._uses_implicit_id_primary_key(pk_cols, column_names):
            declaration = self.implicit_id_declaration()
            self._column_declarations.append(declaration)
            builder = builder.columns(self.make_column(declaration.name, declaration.definition_sql))
            single_inline_pk = True

        for column in self.table.column_specs:
            declaration = self.render_column_declaration(
                column=column,
                pk_columns=pk_cols,
                pk_members=pk_set,
                single_inline_pk=single_inline_pk,
            )
            self._column_declarations.append(declaration)
            builder = builder.columns(self.make_column(column.name, declaration.definition_sql))

        seen_unique: set[tuple[str, ...]] = set()
        for columns in self.table.unique_indexes:
            if columns in seen_unique:
                continue
            seen_unique.add(columns)
            builder = builder.unique(*columns)

        if pk_cols:
            if len(pk_cols) == 1 and not single_inline_pk:
                builder = builder.primary_key(*pk_cols)
            elif len(pk_cols) > 1:
                builder = builder.primary_key(*pk_cols)

        return builder.get_sql(quote_char=self.quote_char) + ';'

    def _uses_implicit_id_primary_key(self, pk_cols: tuple[str, ...], column_names: set[str]) -> bool:
        return pk_cols == ("id",) and "id" not in column_names

    def implicit_id_declaration(self) -> ColumnDeclaration:
        return ColumnDeclaration(
            name="id",
            type_sql="INTEGER",
            definition_sql=self.inline_primary_key_definition("INTEGER"),
            not_null=False,
            primary_key=True,
            auto_increment=True,
        )

    def render_index_definitions(self) -> list[IndexDefinition]:
        definitions: list[IndexDefinition] = []
        specs = [(columns, False) for columns in self.table.indexes]
        specs.extend((columns, True) for columns in self.table.unique_indexes)
        for columns, unique in specs:
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

    def render_column_declaration(
        self,
        *,
        column: ColumnSpec,
        pk_columns: tuple[str, ...],
        pk_members: set[str],
        single_inline_pk: bool,
    ) -> ColumnDeclaration:
        sql_type = self.resolve_column_type(column.python_type)
        if self.use_inline_primary_key(
            column=column,
            pk_columns=pk_columns,
            sql_type=sql_type,
        ):
            definition_sql = self.inline_primary_key_definition(sql_type)
            return ColumnDeclaration(
                name=column.name,
                type_sql=sql_type,
                definition_sql=definition_sql,
                not_null=False,
                primary_key=True,
                auto_increment=True,
            )

        not_null = self.include_not_null(
            column,
            pk_members=pk_members,
            single_inline_pk=single_inline_pk,
        )
        definition_sql = sql_type
        if not_null:
            definition_sql = self.append_not_null(definition_sql)
        primary_key = column.name in pk_members
        return ColumnDeclaration(
            name=column.name,
            type_sql=sql_type,
            definition_sql=definition_sql,
            not_null=not_null,
            primary_key=primary_key,
            auto_increment=False,
        )

    def include_not_null(
        self,
        column: ColumnSpec,
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
        suffix = '_'.join(columns)
        prefix = 'uq' if unique else 'idx'
        return f'{prefix}_{self.table_name}_{suffix}'

    def create_index_sql(self, definition: IndexDefinition) -> str:
        table = Table(self.table_name)
        index = PypikaIndex(definition.name)
        columns_sql = ', '.join(
            table.field(column).get_sql(quote_char=self.quote_char) for column in definition.columns
        )
        unique_keyword = 'UNIQUE ' if definition.unique else ''
        index_sql = (
            f'CREATE {unique_keyword}INDEX IF NOT EXISTS '
            f'{index.get_sql(quote_char=self.quote_char)} '
            f'ON {table.get_sql(quote_char=self.quote_char)} ({columns_sql});'
        )
        return index_sql

    def drop_index_sql(self, index_name: str) -> str:
        index = PypikaIndex(index_name)
        return f'DROP INDEX IF EXISTS {index.get_sql(quote_char=self.quote_char)};'

    def _has_inline_primary_key(self, pk_columns: tuple[str, ...]) -> bool:
        if len(pk_columns) != 1:
            return False
        pk_name = pk_columns[0]
        for column in self.table.column_specs:
            if column.name != pk_name:
                continue
            sql_type = self.resolve_column_type(column.python_type)
            return self.use_inline_primary_key(
                column=column,
                pk_columns=pk_columns,
                sql_type=sql_type,
            )
        return False

    @property
    def table_name(self) -> str:
        if self._table_name_override is not None:
            return self._table_name_override
        return self.table.table_name

    @abstractmethod
    def resolve_column_type(self, annotation: Any) -> str:
        ...

    def use_inline_primary_key(
        self,
        *,
        column: ColumnSpec,
        pk_columns: tuple[str, ...],
        sql_type: str,
    ) -> bool:
        if len(pk_columns) != 1:
            return False
        return False

    def inline_primary_key_definition(self, sql_type: str) -> str:
        raise NotImplementedError('Inline primary key definition not supported for this backend')

    def append_not_null(self, definition: str) -> str:
        return f'{definition} NOT NULL'


class DatabasePusher(ABC):
    schema_builder_cls: type[SchemaBuilder]

    @abstractmethod
    def fetch_existing_indexes(self, conn: Any, table: SchemaTableProtocol) -> set[str]:
        ...

    @abstractmethod
    def execute_statements(self, conn: Any, statements: Iterable[str]) -> None:
        ...

    @abstractmethod
    def table_exists(self, conn: Any, table: SchemaTableProtocol) -> bool:
        ...

    @abstractmethod
    def inspect_existing_schema(
        self,
        conn: Any,
        table: SchemaTableProtocol,
    ) -> tuple[ExistingColumn, ...] | None:
        ...

    def calculate_diff(self, existing: tuple[ExistingColumn, ...], expected: SchemaPlan) -> SchemaDiff:
        existing_map = {column.name: column for column in existing}
        expected_map = {column.name: column for column in expected.columns}

        added = [column for column in expected.columns if column.name not in existing_map]
        removed = [column for column in existing if column.name not in expected_map]

        changed: list[ColumnChange] = []
        for name, current in existing_map.items():
            target = expected_map.get(name)
            if target is None:
                continue
            reasons: list[str] = []
            if current.type_sql.upper() != target.type_sql.upper():
                reasons.append(f"type {current.type_sql} -> {target.type_sql}")
            if current.not_null != target.not_null:
                reasons.append(f"not_null {current.not_null} -> {target.not_null}")
            if current.primary_key != target.primary_key:
                reasons.append(f"primary_key {current.primary_key} -> {target.primary_key}")
            if reasons:
                changed.append(
                    ColumnChange(
                        name=name,
                        current=current,
                        expected=target,
                        reasons=tuple(reasons),
                    )
                )

        return SchemaDiff(
            added=tuple(added),
            removed=tuple(removed),
            changed=tuple(changed),
        )

    def format_diff_message(self, table: SchemaTableProtocol, diff: SchemaDiff) -> str:
        parts: list[str] = [f"模型 {table.table_name} 需要重建表"]
        if diff.added:
            added_desc = ", ".join(f"+{col.name}:{col.type_sql}" for col in diff.added)
            parts.append(f"新增列: {added_desc}")
        if diff.removed:
            removed_desc = ", ".join(f"-{col.name}:{col.type_sql}" for col in diff.removed)
            parts.append(f"删除列: {removed_desc}")
        if diff.changed:
            changed_desc = ", ".join(
                f"~{change.name}({'; '.join(change.reasons)})" for change in diff.changed
            )
            parts.append(f"变更列: {changed_desc}")
        return "; ".join(parts)

    @abstractmethod
    def rebuild_table(
        self,
        conn: Any,
        table: SchemaTableProtocol,
        builder: SchemaBuilder,
        plan: SchemaPlan,
        existing_schema: tuple[ExistingColumn, ...] | None,
        diff: SchemaDiff,
    ) -> None:
        ...

    def is_system_index(self, name: str) -> bool:
        return False

    def validate_connection(self, conn: Any) -> None:
        return None

    def push(
        self,
        conn: Any,
        tables: Sequence[SchemaTableProtocol],
        *,
        sync_indexes: bool = False,
        confirm_rebuild: ConfirmRebuildCallback | None = None,
    ) -> None:
        self.validate_connection(conn)
        for table in tables:
            builder = self.schema_builder_cls(table)
            plan = builder.build()

            if not self.table_exists(conn, table):
                self.execute_statements(conn, [plan.create_sql])
            else:
                existing_schema = self.inspect_existing_schema(conn, table)
                if existing_schema is None:
                    diff = SchemaDiff(added=plan.columns, removed=tuple(), changed=tuple())
                else:
                    diff = self.calculate_diff(existing_schema, plan)

                if existing_schema is None or not diff.is_empty():
                    if confirm_rebuild is None:
                        raise RuntimeError(self.format_diff_message(table, diff))
                    if not confirm_rebuild(table, plan, existing_schema, diff):
                        raise RuntimeError(self.format_diff_message(table, diff))
                    self.rebuild_table(conn, table, builder, plan, existing_schema, diff)

            self._sync_indexes(conn, table, builder, plan.indexes, sync_indexes)

    def _sync_indexes(
        self,
        conn: Any,
        table: SchemaTableProtocol,
        builder: SchemaBuilder,
        index_definitions: tuple[IndexDefinition, ...],
        sync_indexes: bool,
    ) -> None:
        existing_indexes = self.fetch_existing_indexes(conn, table)
        expected_names = {definition.name for definition in index_definitions}

        statements: list[str] = []

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

        if statements:
            self.execute_statements(conn, statements)
