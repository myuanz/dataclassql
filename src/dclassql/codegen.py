from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import MISSING, dataclass, field, fields
from datetime import date, datetime
from enum import Enum
from types import UnionType
from typing import Any, Iterable, Mapping, Sequence, Union, Literal

from jinja2 import Environment, PackageLoader

from .model_inspector import (
    ColumnInfo,
    FieldTo,
    ModelGraph,
    ModelInfo,
    Relationship,
    TypeHint,
)


@dataclass(slots=True)
class GeneratedModule:
    code: str
    asdict_stub: str
    init_code: str
    init_stub: str
    model_names: tuple[str, ...]
    client_class_name: str


@dataclass(slots=True)
class ImportBlock:
    module: str
    names: tuple[str, ...]


@dataclass(slots=True)
class InsertFieldSpec:
    name: str
    annotation: str
    default_expr: str | None


@dataclass(slots=True)
class TypedDictFieldSpec:
    name: str
    annotation: str


@dataclass(slots=True)
class WhereFieldSpec:
    name: str
    annotation: str


@dataclass(slots=True)
class ColumnSpecRender:
    name: str
    '''数据库列名'''

    name_repr: str
    '''列名的 Python 字符串字面量形式, 用于生成代码里的 dict key.'''

    python_type_expr: str
    '''列 Python 类型在生成代码中的表达式.'''

    storage_kind_repr: str
    '''scalar/json 存储类型的字符串字面量.'''

    optional: bool
    '''插入/更新时是否允许 None 或缺省, 来自 Optional/default/factory 判断.'''

    auto_increment: bool
    '''是否自增，给主键用的'''

    has_default: bool
    '''原 dataclass 字段是否有 default'''

    has_default_factory: bool
    '''原 dataclass 字段是否有 default_factory'''

    returned_field: bool
    '''是否会进入返回的原 dataclass 对象. 隐式 id 为 False.'''

    mapping_value_expr: str
    '''Mapping payload 转数据库值的生成表达式. 例如 `data['open_order_id']`.'''

    insert_value_expr: str
    '''Insert dataclass 或原模型实例转数据库值的生成表达式. 隐式 id 用 getattr 默认 None.'''

    is_enum: bool
    '''是否是 Enum 列'''


@dataclass(slots=True)
class DefaultFactoryRender:
    var_name: str
    expression: str


@dataclass(slots=True)
class RowAssignmentRender:
    field_name: str
    value_expr: str


@dataclass(slots=True)
class RelationFilterRender:
    name: str
    fields: tuple[TypedDictFieldSpec, ...]


@dataclass(slots=True)
class ScalarFilterRender:
    name: str
    fields: tuple[TypedDictFieldSpec, ...]


@dataclass(slots=True)
class UpsertWhereRender:
    name: str
    fields: tuple[TypedDictFieldSpec, ...]


@dataclass(slots=True)
class ModelRenderContext:
    name: str
    datasource_expr: str
    table_name_literal: str
    insert_fields: tuple[InsertFieldSpec, ...]
    typed_dict_fields: tuple[TypedDictFieldSpec, ...]
    update_fields: tuple[TypedDictFieldSpec, ...]
    upsert_where_dicts: tuple["UpsertWhereRender", ...]
    dict_fields: tuple[TypedDictFieldSpec, ...]
    where_fields: tuple[WhereFieldSpec, ...]
    relation_filters: tuple[RelationFilterRender, ...]
    column_specs: tuple[ColumnSpecRender, ...]
    relationships: tuple[Relationship, ...]
    primary_key_literal: str
    indexes_literal: str
    unique_indexes_literal: str
    primary_value_types: tuple[str, ...]
    primary_key_on_model: bool
    row_assignments: tuple[RowAssignmentRender, ...]
    default_factories: tuple[DefaultFactoryRender, ...]
    model_info: ModelInfo


@dataclass(slots=True)
class ClientDataSourceContext:
    url_repr: str
    name_repr: str


@dataclass(slots=True)
class ClientModelBindingContext:
    attr_name: str
    model_name: str


@dataclass(slots=True)
class ClientContext:
    class_name: str
    datasource: ClientDataSourceContext
    model_bindings: tuple[ClientModelBindingContext, ...]


@dataclass(slots=True)
class _ModelRenderState:
    info: ModelInfo
    name: str
    model_column_names: set[str]
    db_columns: tuple[ColumnInfo, ...]
    column_lookup: FieldTo[ColumnInfo]
    enum_type_map: FieldTo[type[Enum] | None]
    relationships: tuple[Relationship, ...]


_TEMPLATE_NAME = "client_module.py.jinja"
_ENVIRONMENT: Environment | None = None


def _get_environment() -> Environment:
    global _ENVIRONMENT
    if _ENVIRONMENT is None:
        _ENVIRONMENT = Environment(
            loader=PackageLoader("dclassql", "templates"),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _ENVIRONMENT


class ClientCompiler:
    def __init__(self, graph: ModelGraph, *, client_class_name: str = "GeneratedClient") -> None:
        self.graph = graph
        self.client_class_name = client_class_name
        self.renderer = _TypeRenderer({info.model: name for name, info in graph.by_name.items()})
        self.filter_registry = _ScalarFilterRegistry(self.renderer)

    @classmethod
    def from_models(
        cls,
        models: Sequence[type[Any]],
        *,
        client_class_name: str = "GeneratedClient",
    ) -> "ClientCompiler":
        return cls(ModelGraph.from_models(models), client_class_name=client_class_name)

    def compile(self) -> GeneratedModule:
        model_contexts = [
            self.build_model_context(self.graph.by_name[name])
            for name in sorted(self.graph.by_name)
        ]
        import_blocks = self._build_import_blocks()
        client_context = self.build_client_context()
        exports = self.collect_exports(model_contexts)

        template = _get_environment().get_template(_TEMPLATE_NAME)
        code = template.render(
            module_imports=tuple(import_blocks),
            models=tuple(model_contexts),
            client=client_context,
            exports=tuple(exports),
            scalar_filters=self.filter_registry.render_definitions(),
        )
        if not code.endswith("\n"):
            code += "\n"
        return GeneratedModule(
            code=code,
            asdict_stub=_render_asdict_stub(model_contexts),
            init_code=_render_init_code(self.client_class_name),
            init_stub=_render_init_stub(self.client_class_name),
            model_names=tuple(sorted(self.graph.by_name)),
            client_class_name=self.client_class_name,
        )

    def _build_import_blocks(self) -> list[ImportBlock]:
        imports: defaultdict[str, set[str]] = defaultdict(set)
        for info in self.graph.by_name.values():
            imports[info.model.__module__].add(info.model.__name__)
        for module, names in self.renderer.module_imports.items():
            imports[module].update(names)
        return [
            ImportBlock(module=module, names=tuple(sorted(names)))
            for module, names in sorted(imports.items())
        ]

    def _model_state(self, info: ModelInfo) -> _ModelRenderState:
        db_columns = _build_db_columns(info)
        return _ModelRenderState(
            info=info,
            name=info.model.__name__,
            model_column_names={column.name for column in info.columns},
            db_columns=db_columns,
            column_lookup=FieldTo.from_mapping({column.name: column for column in db_columns}),
            enum_type_map=FieldTo.from_mapping({
                column.name: column.type_hint.enum_type
                for column in db_columns
            }),
            relationships=self.graph.relationships.by_model(info.model),
        )

    def build_model_context(self, info: ModelInfo) -> ModelRenderContext:
        state = self._model_state(info)
        insert_fields, typed_dict_fields, update_fields, dict_field_map = self._build_column_fields(state)
        upsert_where_dicts = self._build_upsert_where(state)
        where_fields, relation_filters = self._build_where_fields(state)
        row_assignments, default_factories = self._build_row_assignments(state)
        datasource = info.datasource
        constraints = info.constraints
        primary_key = constraints.primary_key.names
        indexes = tuple(group.names for group in constraints.indexes)
        unique_indexes = tuple(group.names for group in constraints.unique_indexes)

        return ModelRenderContext(
            name=state.name,
            datasource_expr=f"DataSourceConfig(url={datasource.url!r}, name={datasource.name!r})",
            table_name_literal=repr(state.name),
            insert_fields=tuple(insert_fields),
            typed_dict_fields=tuple(typed_dict_fields),
            update_fields=tuple(update_fields),
            upsert_where_dicts=tuple(upsert_where_dicts),
            dict_fields=tuple(self._build_dict_fields(state, dict_field_map)),
            where_fields=tuple(where_fields),
            relation_filters=tuple(relation_filters),
            column_specs=tuple(self._build_column_specs(state)),
            relationships=state.relationships,
            primary_key_literal=_tuple_literal(primary_key),
            indexes_literal=_tuple_literal(indexes) if indexes else "()",
            unique_indexes_literal=_tuple_literal(unique_indexes) if unique_indexes else "()",
            primary_value_types=tuple(
                self.renderer.render(state.column_lookup[name].type_hint.source)
                for name in primary_key
            ),
            primary_key_on_model=all(name in state.model_column_names for name in primary_key),
            row_assignments=tuple(row_assignments),
            default_factories=tuple(default_factories),
            model_info=info,
        )

    def _build_column_fields(
        self,
        state: _ModelRenderState,
    ) -> tuple[
        list[InsertFieldSpec],
        list[TypedDictFieldSpec],
        list[TypedDictFieldSpec],
        FieldTo[str],
    ]:
        insert_fields: list[InsertFieldSpec] = []
        typed_dict_fields: list[TypedDictFieldSpec] = []
        update_fields: list[TypedDictFieldSpec] = []
        dict_field_map: dict[str, str] = {}
        for column in state.db_columns:
            annotation = _format_insert_annotation(column, self.renderer)
            default_expr = _render_default_fragment(state.info.model, column)
            if default_expr is None and column.auto_increment:
                default_expr = "None"
            insert_fields.append(InsertFieldSpec(column.name, annotation, default_expr))

            if column.auto_increment:
                self.renderer.require_typing("NotRequired")
                typed_annotation = f"NotRequired[{_strip_optional_annotation(annotation)}]"
            else:
                typed_annotation = annotation
            typed_dict_fields.append(TypedDictFieldSpec(column.name, typed_annotation))

            rendered_type = self.renderer.render(column.type_hint.source)
            dict_field_map[column.name] = rendered_type
            update_fields.append(TypedDictFieldSpec(column.name, rendered_type))
        return (
            insert_fields,
            typed_dict_fields,
            update_fields,
            FieldTo.from_mapping(dict_field_map),
        )

    def _build_upsert_where(self, state: _ModelRenderState) -> list[UpsertWhereRender]:
        result: list[UpsertWhereRender] = []
        primary_key = state.info.constraints.primary_key.names
        if primary_key:
            fields = tuple(
                TypedDictFieldSpec(
                    name,
                    self.renderer.render(state.column_lookup[name].type_hint.source),
                )
                for name in primary_key
            )
            result.append(UpsertWhereRender(f"{state.name}UpsertWherePK", fields))
        for index, group in enumerate(state.info.constraints.unique_indexes, start=1):
            fields = tuple(
                TypedDictFieldSpec(
                    name,
                    self.renderer.render(state.column_lookup[name].type_hint.source)
                    if name in state.column_lookup
                    else "object",
                )
                for name in group.names
            )
            result.append(UpsertWhereRender(f"{state.name}UpsertWhereUnique{index}", fields))
        if not result:
            self.renderer.require_typing("Never")
        return result

    def _build_where_fields(
        self,
        state: _ModelRenderState,
    ) -> tuple[list[WhereFieldSpec], list[RelationFilterRender]]:
        where_fields: list[WhereFieldSpec] = []
        for column in state.db_columns:
            annotation = self.renderer.render(column.type_hint.source)
            if "None" not in annotation:
                annotation = f"{annotation} | None"
            filter_name = self.filter_registry.register(column.type_hint)
            if filter_name is not None and filter_name not in annotation:
                annotation = f"{annotation} | {filter_name}"
            where_fields.append(WhereFieldSpec(column.name, annotation))

        relation_filters: list[RelationFilterRender] = []
        for relationship in state.relationships:
            local = relationship.local
            filter_name = f"{state.name}{_to_pascal_case(local.attribute)}RelationFilter"
            remote_where = f"{local.target.__name__}WhereDict"
            if local.many:
                fields_ = (
                    TypedDictFieldSpec("SOME", f"{remote_where} | None"),
                    TypedDictFieldSpec("NONE", f"{remote_where} | None"),
                    TypedDictFieldSpec("EVERY", remote_where),
                )
            else:
                fields_ = (
                    TypedDictFieldSpec("IS", f"{remote_where} | None"),
                    TypedDictFieldSpec("IS_NOT", f"{remote_where} | None"),
                )
            relation_filters.append(RelationFilterRender(filter_name, fields_))
            where_fields.append(WhereFieldSpec(local.attribute, filter_name))

        self.renderer.require_typing("Sequence")
        where_dict = f"{state.name}WhereDict"
        where_fields.extend(
            (
                WhereFieldSpec("AND", f"{where_dict} | Sequence[{where_dict}]"),
                WhereFieldSpec("OR", f"Sequence[{where_dict}]"),
                WhereFieldSpec("NOT", f"{where_dict} | Sequence[{where_dict}]"),
            )
        )
        return where_fields, relation_filters

    def _build_column_specs(self, state: _ModelRenderState) -> list[ColumnSpecRender]:
        return [
            ColumnSpecRender(
                name=column.name,
                name_repr=repr(column.name),
                python_type_expr=self.renderer.render(column.type_hint.source),
                storage_kind_repr=repr(column.storage_kind),
                optional=column.optional,
                auto_increment=column.auto_increment,
                has_default=column.has_default,
                has_default_factory=column.has_default_factory,
                returned_field=column.name in state.model_column_names,
                mapping_value_expr=_format_mapping_value_expr(column, state.enum_type_map[column.name]),
                insert_value_expr=_format_insert_value_expr(
                    column,
                    state.enum_type_map[column.name],
                    returned_field=column.name in state.model_column_names,
                ),
                is_enum=state.enum_type_map[column.name] is not None,
            )
            for column in state.db_columns
        ]

    def _build_dict_fields(
        self,
        state: _ModelRenderState,
        column_types: FieldTo[str],
    ) -> list[TypedDictFieldSpec]:
        relations = FieldTo.from_mapping({
            relationship.local.attribute: relationship
            for relationship in state.relationships
        })
        result: list[TypedDictFieldSpec] = []
        for field_obj in fields(state.info.model):
            if field_obj.name in column_types:
                result.append(TypedDictFieldSpec(field_obj.name, column_types[field_obj.name]))
                continue
            relation = relations.get(field_obj.name)
            if relation is not None:
                local = relation.local
                target = f"{local.target.__name__}Dict"
                annotation = f"list[{target}]" if local.many else f"{target} | None"
                result.append(TypedDictFieldSpec(field_obj.name, annotation))
                continue
            result.append(TypedDictFieldSpec(field_obj.name, self.renderer.render(field_obj.type)))
        return result

    def _build_row_assignments(
        self,
        state: _ModelRenderState,
    ) -> tuple[list[RowAssignmentRender], list[DefaultFactoryRender]]:
        column_map = FieldTo.from_mapping({column.name: column for column in state.info.columns})
        relation_defaults = FieldTo.from_mapping({
            relationship.local.attribute: (
                "[]" if relationship.local.many else "None"
            )
            for relationship in state.relationships
        })
        assignments: list[RowAssignmentRender] = []
        factories: list[DefaultFactoryRender] = []
        for field_obj in fields(state.info.model):
            expression, factory = self._resolve_row_assignment(
                state.info.model,
                field_obj,
                column_map,
                state.enum_type_map,
                relation_defaults,
            )
            assignments.append(RowAssignmentRender(field_obj.name, expression))
            if factory is not None:
                factories.append(factory)
        return assignments, factories

    def _resolve_row_assignment(
        self,
        model: type[Any],
        field_obj: Any,
        column_map: FieldTo[ColumnInfo],
        enum_types: FieldTo[type[Enum] | None],
        relation_defaults: FieldTo[str],
    ) -> tuple[str, DefaultFactoryRender | None]:
        column = column_map.get(field_obj.name)
        if column is not None:
            return self._column_value_expression(column, enum_types.get(field_obj.name)), None
        if field_obj.default is not MISSING:
            return f"{model.__name__}.__dataclass_fields__[{field_obj.name!r}].default", None
        if field_obj.default_factory is not MISSING:
            name = f"_{model.__name__}_{field_obj.name}_default_factory"
            expression = f"{model.__name__}.__dataclass_fields__[{field_obj.name!r}].default_factory"
            return f"{name}()", DefaultFactoryRender(name, expression)
        if field_obj.name in relation_defaults:
            return relation_defaults[field_obj.name], None
        return _infer_field_fallback(TypeHint.parse(field_obj.type)), None

    def _column_value_expression(self, column: ColumnInfo, enum_type: type[Enum] | None) -> str:
        value = f"row[{column.name!r}]"
        if column.storage_kind == "json":
            return f"deserialize_json_value({value}, {self.renderer.render(column.type_hint.source)})"
        if enum_type is None:
            return value
        converted = f"{enum_type.__name__}({value})"
        return f"({converted} if {value} is not None else None)" if column.optional else converted

    def build_client_context(self) -> ClientContext:
        datasources = {info.datasource for info in self.graph.by_name.values()}
        if len(datasources) != 1:
            labels = ", ".join(
                f"{datasource.identity}({datasource.url!r})"
                for datasource in sorted(datasources, key=lambda item: item.identity)
            )
            raise ValueError(f"Generated Client can only use one datasource, got: {labels}")
        datasource = next(iter(datasources))
        return ClientContext(
            class_name=self.client_class_name,
            datasource=ClientDataSourceContext(repr(datasource.url), repr(datasource.name)),
            model_bindings=tuple(
                ClientModelBindingContext(_camel_to_snake(name), name)
                for name in sorted(self.graph.by_name)
            ),
        )

    def collect_exports(self, contexts: Sequence[ModelRenderContext]) -> list[str]:
        exports = ["DataSourceConfig", "TableRelation", self.client_class_name]
        for context in contexts:
            name = context.name
            exports.extend(
                (
                    f"T{name}IncludeCol",
                    f"T{name}SortableCol",
                    f"T{name}DistinctCol",
                    f"{name}IncludeDict",
                    f"{name}OrderByDict",
                    f"{name}Dict",
                    f"{name}Insert",
                    f"{name}InsertDict",
                    f"{name}UpdateDict",
                    f"{name}UpsertWhereDict",
                    f"{name}WhereDict",
                    f"{name}Table",
                )
            )
            exports.extend(relation_filter.name for relation_filter in context.relation_filters)
        return exports


def generate_client(models: Sequence[type[Any]], *, client_class_name: str = "GeneratedClient") -> GeneratedModule:
    return ClientCompiler.from_models(models, client_class_name=client_class_name).compile()


def _build_db_columns(info: ModelInfo) -> tuple[ColumnInfo, ...]:
    if info.constraints.primary_key.names == ("id",) and all(
        column.name != "id" for column in info.columns
    ):
        implicit_id = ColumnInfo(
            name="id",
            type_hint=TypeHint.parse(int),
            optional=False,
            auto_increment=True,
            storage_kind="scalar",
            has_default=False,
            default_value=None,
            has_default_factory=False,
            default_factory=None,
        )
        return (implicit_id, *info.columns)
    return tuple(info.columns)


def _render_asdict_stub(model_contexts: Sequence[ModelRenderContext]) -> str:
    template = _get_environment().get_template("asdict_stub.pyi.jinja")
    code = template.render(models=tuple(model_contexts))
    if not code.endswith("\n"):
        code += "\n"
    return code


def _render_init_code(client_class_name: str) -> str:
    code = (
        "from dclassql.asdict import asdict as asdict\n"
        f"from .client import {client_class_name} as {client_class_name}\n\n"
        f"__all__ = ['{client_class_name}', 'asdict']\n"
    )
    return code


def _render_init_stub(client_class_name: str) -> str:
    code = (
        "from .asdict import asdict as asdict\n"
        f"from .client import {client_class_name} as {client_class_name}\n\n"
        "__all__: list[str]\n"
    )
    return code


def _format_mapping_value_expr(column: ColumnInfo, enum_type: type[Enum] | None) -> str:
    if column.storage_kind == "json":
        return f"serialize_json_value(data[{column.name!r}])"
    if enum_type is None:
        return f"data[{column.name!r}]"
    value_expr = f"data[{column.name!r}]"
    if column.optional:
        return f"({value_expr}.value if {value_expr} is not None else None)"
    return f"{value_expr}.value"


def _format_insert_value_expr(
    column: ColumnInfo,
    enum_type: type[Enum] | None,
    *,
    returned_field: bool = True,
) -> str:
    if not returned_field:
        return f"getattr(data, {column.name!r}, None)"
    if column.storage_kind == "json":
        return f"serialize_json_value(data.{column.name})"
    if enum_type is None:
        return f"data.{column.name}"
    value_expr = f"data.{column.name}"
    if column.optional:
        return f"({value_expr}.value if {value_expr} is not None else None)"
    return f"{value_expr}.value"


def _infer_field_fallback(type_hint: TypeHint) -> str:
    type_hint = type_hint.without_optional()
    candidate = type_hint.origin or type_hint.annotation
    collection_map: dict[type[Any], str] = {list: "list", set: "set", frozenset: "frozenset"}
    if isinstance(candidate, type) and candidate in collection_map:
        return f"{collection_map[candidate]}()"
    return "None"


def _format_insert_annotation(col: ColumnInfo, renderer: "_TypeRenderer") -> str:
    annotation = renderer.render(col.type_hint.source)
    needs_optional = col.auto_increment
    if needs_optional and "None" not in annotation:
        annotation = f"{annotation} | None"
    return annotation


def _render_default_fragment(model_cls: type[Any], col: ColumnInfo) -> str | None:
    if col.has_default_factory and col.default_factory is not None:
        factory_expr = f"{model_cls.__name__}.__dataclass_fields__['{col.name}'].default_factory"
        return f"field(default_factory={factory_expr})"
    if col.has_default:
        return f"{model_cls.__name__}.__dataclass_fields__['{col.name}'].default"
    return None


def _literal_expression(values: Sequence[str]) -> str:
    unique = list(dict.fromkeys(values))
    if not unique:
        return "Literal[()]"
    items = ", ".join(repr(value) for value in unique)
    return f"Literal[{items}]"


def _tuple_literal(values: Iterable[Any]) -> str:
    items = list(values)
    if not items:
        return "()"
    if all(isinstance(item, (tuple, list)) for item in items):
        parts = []
        for item in items:
            parts.append(_tuple_literal(item))
        joined = ", ".join(parts)
        return f"({joined},)"
    joined = ", ".join(repr(item) for item in items)
    if len(items) == 1:
        return f"({joined},)"
    return f"({joined})"


def _sanitize_identifier(value: str) -> str:
    result_chars: list[str] = []
    for char in value:
        if char.isalnum() or char == "_":
            result_chars.append(char.lower())
        else:
            result_chars.append("_")
    identifier = "".join(result_chars).replace("__", "_")
    if not identifier or identifier[0].isdigit():
        identifier = f"ds_{identifier}" if identifier else "ds"
    return identifier


def _strip_optional_annotation(annotation: str) -> str:
    parts = [part.strip() for part in annotation.split("|")]
    filtered = [part for part in parts if part != "None"]
    return filtered[0] if len(filtered) == 1 else " | ".join(filtered)


def _camel_to_snake(name: str) -> str:
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    return pattern.sub("_", name).lower()


def _to_pascal_case(value: str) -> str:
    parts = re.split(r"[^0-9a-zA-Z]+", value)
    return "".join(part[:1].upper() + part[1:] for part in parts if part)


@dataclass(slots=True)
class _FilterFieldTemplate:
    name: str
    annotation_template: str
    require_sequence: bool = False


@dataclass(slots=True)
class _FilterTemplate:
    alias: str
    fields: tuple[_FilterFieldTemplate, ...]


_SCALAR_FILTER_TEMPLATES: dict[type[Any], _FilterTemplate] = {
    str: _FilterTemplate(
        alias="StringFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("LT", "{base}"),
            _FilterFieldTemplate("LTE", "{base}"),
            _FilterFieldTemplate("GT", "{base}"),
            _FilterFieldTemplate("GTE", "{base}"),
            _FilterFieldTemplate("CONTAINS", "{base}"),
            _FilterFieldTemplate("STARTS_WITH", "{base}"),
            _FilterFieldTemplate("ENDS_WITH", "{base}"),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
    bool: _FilterTemplate(
        alias="BoolFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
    int: _FilterTemplate(
        alias="IntFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("LT", "{base}"),
            _FilterFieldTemplate("LTE", "{base}"),
            _FilterFieldTemplate("GT", "{base}"),
            _FilterFieldTemplate("GTE", "{base}"),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
    float: _FilterTemplate(
        alias="FloatFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("LT", "{base}"),
            _FilterFieldTemplate("LTE", "{base}"),
            _FilterFieldTemplate("GT", "{base}"),
            _FilterFieldTemplate("GTE", "{base}"),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
    datetime: _FilterTemplate(
        alias="DateTimeFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("LT", "{base}"),
            _FilterFieldTemplate("LTE", "{base}"),
            _FilterFieldTemplate("GT", "{base}"),
            _FilterFieldTemplate("GTE", "{base}"),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
    date: _FilterTemplate(
        alias="DateFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("LT", "{base}"),
            _FilterFieldTemplate("LTE", "{base}"),
            _FilterFieldTemplate("GT", "{base}"),
            _FilterFieldTemplate("GTE", "{base}"),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
    bytes: _FilterTemplate(
        alias="BytesFilter",
        fields=(
            _FilterFieldTemplate("EQ", "{base} | None"),
            _FilterFieldTemplate("IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT_IN", "Sequence[{base}]", require_sequence=True),
            _FilterFieldTemplate("NOT", "{self} | {base} | None"),
        ),
    ),
}


class _ScalarFilterRegistry:
    def __init__(self, renderer: "_TypeRenderer") -> None:
        self._renderer = renderer
        self._definitions: dict[str, ScalarFilterRender] = {}

    def register(self, type_hint: TypeHint) -> str | None:
        base_type = type_hint.scalar_base
        if base_type is None:
            return None
        template = _SCALAR_FILTER_TEMPLATES.get(base_type)
        if template is None:
            return None
        if template.alias not in self._definitions:
            base_annotation = self._renderer.render(base_type)
            fields: list[TypedDictFieldSpec] = []
            for field_template in template.fields:
                field_annotation = field_template.annotation_template.format(
                    base=base_annotation,
                    self=template.alias,
                )
                fields.append(TypedDictFieldSpec(name=field_template.name, annotation=field_annotation))
                if field_template.require_sequence:
                    self._renderer.require_typing("Sequence")
            self._definitions[template.alias] = ScalarFilterRender(
                name=template.alias,
                fields=tuple(fields),
            )
        return template.alias

    def render_definitions(self) -> tuple[ScalarFilterRender, ...]:
        return tuple(self._definitions[name] for name in sorted(self._definitions))


class _TypeRenderer:
    def __init__(self, model_map: Mapping[type[Any], str]) -> None:
        self._model_map = dict(model_map)
        self._module_imports: defaultdict[str, set[str]] = defaultdict(set) # {module: set of names}
        self._typing_imports: set[str] = set()

    def render(self, tp: Any) -> str:
        alias_value = getattr(tp, "__value__", None)
        if alias_value is not None:
            alias_name = getattr(tp, "__name__", None)
            alias_module = getattr(tp, "__module__", None)
            if isinstance(alias_name, str) and isinstance(alias_module, str):
                self._module_imports[alias_module].add(alias_name)
                return alias_name
            return self.render(alias_value)
        type_hint = TypeHint.parse(tp)
        tp = type_hint.annotation
        origin = type_hint.origin
        args = type_hint.args
        if tp is Any:
            return "Any"
        if tp is type(None):
            return "None"
        if origin in (UnionType, Union):
            parts = [self.render(arg) for arg in args]
            return " | ".join(dict.fromkeys(parts))
        if origin is Literal:
            self._typing_imports.add("Literal")
            values = ", ".join(repr(value) for value in args)
            return f"Literal[{values}]"
        if origin in (list, set, frozenset):
            args = args or (Any,)
            if origin is set:
                container = "set"
            elif origin is frozenset:
                container = "frozenset"
            else:
                container = "list"
            return f"{container}[{self.render(args[0])}]"
        if origin is tuple:
            if len(args) == 2 and args[1] is Ellipsis:
                return f"tuple[{self.render(args[0])}, ...]"
            return f"tuple[{', '.join(self.render(arg) for arg in args)}]"
        if origin is dict:
            key, value = args or (Any, Any)
            return f"dict[{self.render(key)}, {self.render(value)}]"
        if origin is None:
            pass
        if isinstance(tp, type):
            mapped = self._model_map.get(tp)
            if mapped is not None:
                return mapped
            if tp.__module__ == "builtins":
                return tp.__name__
            if tp.__module__ == "datetime":
                self._module_imports["datetime"].add(tp.__name__)
                return tp.__name__
            self._module_imports[tp.__module__].add(tp.__qualname__.split(".")[0])
            return tp.__qualname__
        return repr(tp)

    @property
    def module_imports(self) -> defaultdict[str, set[str]]:
        return self._module_imports

    @property
    def typing_names(self) -> set[str]:
        return set(self._typing_imports)

    def require_typing(self, name: str) -> None:
        self._typing_imports.add(name)
