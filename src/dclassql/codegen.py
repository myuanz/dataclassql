from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from types import UnionType
from typing import Annotated, Any, Iterable, Mapping, Sequence, get_args, get_origin, Literal

from jinja2 import Environment, PackageLoader

from .model_inspector import ColumnInfo, ModelInfo, inspect_models, DataSourceConfig


@dataclass(slots=True)
class GeneratedModule:
    code: str
    model_names: tuple[str, ...]


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
    name_repr: str
    optional: bool
    auto_increment: bool
    has_default: bool
    has_default_factory: bool


@dataclass(slots=True)
class ForeignKeyRender:
    local_columns_literal: str
    remote_model: str
    remote_columns_literal: str
    backref_repr: str


@dataclass(slots=True)
class RelationRender:
    name_repr: str
    table_name_repr: str
    many: bool
    mapping_literal: str
    table_module_expr: str
    table_factory_expr: str


@dataclass(slots=True)
class RelationFilterRender:
    name: str
    fields: tuple[TypedDictFieldSpec, ...]


@dataclass(slots=True)
class ScalarFilterRender:
    name: str
    fields: tuple[TypedDictFieldSpec, ...]


@dataclass(slots=True)
class ModelRenderContext:
    name: str
    datasource_expr: str
    table_name_literal: str
    insert_fields: tuple[InsertFieldSpec, ...]
    typed_dict_fields: tuple[TypedDictFieldSpec, ...]
    where_fields: tuple[WhereFieldSpec, ...]
    relation_filters: tuple[RelationFilterRender, ...]
    column_specs: tuple[ColumnSpecRender, ...]
    foreign_keys: tuple[ForeignKeyRender, ...]
    relation_entries: tuple[RelationRender, ...]
    primary_key_literal: str
    indexes_literal: str
    unique_indexes_literal: str
    model_info: ModelInfo


@dataclass(slots=True)
class ClientDatasourceContext:
    key: str
    key_repr: str
    provider_repr: str
    url_repr: str
    name_repr: str


@dataclass(slots=True)
class BackendMethodContext:
    key: str
    key_repr: str
    method_name: str


@dataclass(slots=True)
class ClientModelBindingContext:
    attr_name: str
    model_name: str
    backend_method: str


@dataclass(slots=True)
class ClientContext:
    datasource_items: tuple[ClientDatasourceContext, ...]
    backend_methods: tuple[BackendMethodContext, ...]
    model_bindings: tuple[ClientModelBindingContext, ...]


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


def generate_client(models: Sequence[type[Any]]) -> GeneratedModule:
    model_infos = inspect_models(models)
    renderer = _TypeRenderer({info.model: name for name, info in model_infos.items()})
    filter_registry = _ScalarFilterRegistry(renderer)

    model_imports: dict[str, set[str]] = defaultdict(set)
    for info in model_infos.values():
        module = info.model.__module__
        model_imports.setdefault(module, set()).add(info.model.__name__)

    model_contexts = [
        _build_model_context(model_infos[name], renderer, model_infos, filter_registry)
        for name in sorted(model_infos.keys())
    ]

    module_imports = renderer.build_imports()
    combined_imports: dict[str, set[str]] = defaultdict(set)
    for module, names in model_imports.items():
        combined_imports[module].update(names)
    for module, names in module_imports.items():
        combined_imports[module].update(names)

    import_blocks = [
        ImportBlock(module=module, names=tuple(sorted(names)))
        for module, names in sorted(combined_imports.items())
    ]

    client_context = _build_client_context(model_infos)
    exports = _collect_exports(model_contexts)
    scalar_filters = filter_registry.render_definitions()

    template = _get_environment().get_template(_TEMPLATE_NAME)
    code = template.render(
        module_imports=tuple(import_blocks),
        models=tuple(model_contexts),
        client=client_context,
        exports=tuple(exports),
        scalar_filters=scalar_filters,
    )
    if not code.endswith("\n"):
        code += "\n"
    return GeneratedModule(code=code, model_names=tuple(sorted(model_infos.keys())))


def _build_model_context(
    info: ModelInfo,
    renderer: "_TypeRenderer",
    model_infos: Mapping[str, ModelInfo],
    filter_registry: _ScalarFilterRegistry,
) -> ModelRenderContext:
    name = info.model.__name__

    insert_fields: list[InsertFieldSpec] = []
    typed_dict_fields: list[TypedDictFieldSpec] = []
    for col in info.columns:
        annotation = _format_insert_annotation(col, renderer)
        default_fragment = _render_default_fragment(name, col)
        if default_fragment is not None:
            default_expr = default_fragment
        elif col.auto_increment:
            default_expr = "None"
        else:
            default_expr = None
        insert_fields.append(InsertFieldSpec(name=col.name, annotation=annotation, default_expr=default_expr))

        if col.auto_increment:
            renderer.require_typing("NotRequired")
            base_annotation = _strip_optional_annotation(annotation)
            typed_annotation = f"NotRequired[{base_annotation}]"
        else:
            typed_annotation = annotation
        typed_dict_fields.append(TypedDictFieldSpec(name=col.name, annotation=typed_annotation))

    where_fields: list[WhereFieldSpec] = []
    for col in info.columns:
        annotation = renderer.render(col.python_type)
        if "None" not in annotation:
            annotation = f"{annotation} | None"
        filter_name = filter_registry.register(col.python_type)
        if filter_name is not None and filter_name not in annotation:
            annotation = f"{annotation} | {filter_name}"
        where_fields.append(WhereFieldSpec(name=col.name, annotation=annotation))

    relation_filters: list[RelationFilterRender] = []
    for relation in info.relations:
        filter_name = f"{name}{_to_pascal_case(relation.name)}RelationFilter"
        remote_where_dict = f"{relation.target.__name__}WhereDict"
        if relation.many:
            fields = (
                TypedDictFieldSpec(name="SOME", annotation=f"{remote_where_dict} | None"),
                TypedDictFieldSpec(name="NONE", annotation=f"{remote_where_dict} | None"),
                TypedDictFieldSpec(name="EVERY", annotation=remote_where_dict),
            )
        else:
            fields = (
                TypedDictFieldSpec(name="IS", annotation=f"{remote_where_dict} | None"),
                TypedDictFieldSpec(name="IS_NOT", annotation=f"{remote_where_dict} | None"),
            )
        relation_filters.append(RelationFilterRender(name=filter_name, fields=fields))
        where_fields.append(WhereFieldSpec(name=relation.name, annotation=filter_name))

    renderer.require_typing("Sequence")
    where_dict_name = f"{name}WhereDict"
    where_fields.extend(
        [
            WhereFieldSpec(name="AND", annotation=f"{where_dict_name} | Sequence[{where_dict_name}]"),
            WhereFieldSpec(name="OR", annotation=f"Sequence[{where_dict_name}]"),
            WhereFieldSpec(name="NOT", annotation=f"{where_dict_name} | Sequence[{where_dict_name}]"),
        ]
    )

    column_specs = [
        ColumnSpecRender(
            name_repr=repr(column.name),
            optional=column.optional,
            auto_increment=column.auto_increment,
            has_default=column.has_default,
            has_default_factory=column.has_default_factory,
        )
        for column in info.columns
    ]

    foreign_keys = [
        ForeignKeyRender(
            local_columns_literal=_tuple_literal(fk.local_columns),
            remote_model=fk.remote_model.__name__,
            remote_columns_literal=_tuple_literal(fk.remote_columns),
            backref_repr=repr(fk.backref_attribute),
        )
        for fk in info.foreign_keys
    ]

    relation_entries = [
        RelationRender(
            name_repr=repr(entry["name"]),
            table_name_repr=repr(entry["table_name"]),
            many=entry["many"],
            mapping_literal=_tuple_literal(entry["mapping"]),
            table_module_expr=entry["table_module_expr"],
            table_factory_expr=entry["table_factory_expr"],
        )
        for entry in _build_relation_entries(info, model_infos)
    ]

    datasource_values = info.datasource
    datasource_expr = (
        f"DataSourceConfig(provider={datasource_values.provider!r}, url={repr(datasource_values.url)}, name={repr(datasource_values.name)})"
    )

    indexes_literal = _tuple_literal(tuple(tuple(idx) for idx in info.indexes)) if info.indexes else "()"
    unique_indexes_literal = (
        _tuple_literal(tuple(tuple(idx) for idx in info.unique_indexes)) if info.unique_indexes else "()"
    )

    return ModelRenderContext(
        name=name,
        datasource_expr=datasource_expr,
        table_name_literal=repr(name),
        insert_fields=tuple(insert_fields),
        typed_dict_fields=tuple(typed_dict_fields),
        where_fields=tuple(where_fields),
        relation_filters=tuple(relation_filters),
        column_specs=tuple(column_specs),
        foreign_keys=tuple(foreign_keys),
        relation_entries=tuple(relation_entries),
        primary_key_literal=_tuple_literal(info.primary_key),
        indexes_literal=indexes_literal,
        unique_indexes_literal=unique_indexes_literal,
        model_info=info,
    )


def _build_client_context(model_infos: Mapping[str, ModelInfo]) -> ClientContext:
    datasource_configs: dict[str, DataSourceConfig] = {}
    for info in model_infos.values():
        datasource = info.datasource
        key = datasource.name or datasource.provider
        existing = datasource_configs.get(key)
        if existing is None:
            datasource_configs[key] = datasource
        elif existing != datasource:
            raise ValueError(f"Conflicting datasource key '{key}' for providers")

    datasource_items = [
        ClientDatasourceContext(
            key=key,
            key_repr=repr(key),
            provider_repr=repr(ds.provider),
            url_repr=repr(ds.url),
            name_repr=repr(ds.name),
        )
        for key, ds in sorted(datasource_configs.items())
    ]

    backend_methods: list[BackendMethodContext] = []
    method_map: dict[str, str] = {}
    for key in sorted(datasource_configs.keys()):
        method_name = f"_backend_{_sanitize_identifier(key)}"
        backend_methods.append(BackendMethodContext(key=key, key_repr=repr(key), method_name=method_name))
        method_map[key] = method_name

    model_bindings = [
        ClientModelBindingContext(
            attr_name=_camel_to_snake(name),
            model_name=name,
            backend_method=method_map[(model_infos[name].datasource.name or model_infos[name].datasource.provider)],
        )
        for name in sorted(model_infos.keys())
    ]

    return ClientContext(
        datasource_items=tuple(datasource_items),
        backend_methods=tuple(backend_methods),
        model_bindings=tuple(model_bindings),
    )


def _collect_exports(model_contexts: Sequence[ModelRenderContext]) -> list[str]:
    exports: list[str] = ["DataSourceConfig", "ForeignKeySpec", "Client"]
    for context in model_contexts:
        name = context.name
        exports.extend(
            [
                f"T{name}IncludeCol",
                f"T{name}SortableCol",
                f"{name}IncludeDict",
                f"{name}OrderByDict",
                f"{name}Insert",
                f"{name}InsertDict",
                f"{name}WhereDict",
                f"{name}Table",
            ]
        )
        for relation_filter in context.relation_filters:
            exports.append(relation_filter.name)
    return exports


def _build_relation_entries(info: ModelInfo, model_infos: Mapping[str, ModelInfo]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not info.relations:
        return entries

    target_index: dict[str, ModelInfo] = {name: model for name, model in model_infos.items()}

    for relation in info.relations:
        target_model = relation.target
        target_info = target_index.get(target_model.__name__)
        if target_info is None:
            continue

        mapping: tuple[tuple[str, str], ...] | None = None
        if not relation.many:
            for fk in info.foreign_keys:
                if fk.remote_model is target_model:
                    mapping = tuple((local, remote) for local, remote in zip(fk.local_columns, fk.remote_columns))
                    break
            if mapping is None:
                for fk in target_info.foreign_keys:
                    if fk.remote_model is info.model and fk.backref_attribute == relation.name:
                        mapping = tuple((remote, local) for remote, local in zip(fk.remote_columns, fk.local_columns))
                        break
        else:
            for fk in target_info.foreign_keys:
                if fk.remote_model is info.model and fk.backref_attribute == relation.name:
                    mapping = tuple((remote, local) for remote, local in zip(fk.remote_columns, fk.local_columns))
                    break
        if mapping is None:
            continue
        if target_model.__module__ == info.model.__module__:
            module_expr = "__name__"
        else:
            module_expr = repr(target_model.__module__)
        table_class_name = f"{target_model.__name__}Table"
        entries.append(
            {
                "name": relation.name,
                "table_name": table_class_name,
                "many": relation.many,
                "mapping": mapping,
                "table_module_expr": module_expr,
                "table_factory_expr": f"lambda: {table_class_name}",
            }
        )
    return entries


def _format_insert_annotation(col: ColumnInfo, renderer: "_TypeRenderer") -> str:
    annotation = renderer.render(col.python_type)
    needs_optional = col.auto_increment
    if needs_optional and "None" not in annotation:
        annotation = f"{annotation} | None"
    return annotation


def _render_default_fragment(model_name: str, col: ColumnInfo) -> str | None:
    if col.has_default_factory and col.default_factory is not None:
        factory_expr = f"{model_name}.__dataclass_fields__['{col.name}'].default_factory"
        return f"field(default_factory={factory_expr})"
    if col.has_default:
        return repr(col.default_value)
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


def _resolve_scalar_base(tp: Any) -> type[Any] | None:
    origin = get_origin(tp)
    if origin is Annotated:
        return _resolve_scalar_base(get_args(tp)[0])
    if origin is UnionType:
        args = [arg for arg in get_args(tp) if arg is not type(None)]
        if len(args) == 1:
            return _resolve_scalar_base(args[0])
        return None
    if origin is not None:
        return None
    if isinstance(tp, type):
        if tp is bool:
            return bool
        if issubclass(tp, str):
            return str
        if issubclass(tp, bytes):
            return bytes
        if issubclass(tp, datetime):
            return datetime
        if issubclass(tp, date):
            return date
        if issubclass(tp, float):
            return float
        if issubclass(tp, int):
            return int
    return None


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

    def register(self, annotation: Any) -> str | None:
        base_type = _resolve_scalar_base(annotation)
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
        self._module_imports: dict[str, set[str]] = defaultdict(set)
        self._typing_imports: set[str] = set()

    def render(self, tp: Any) -> str:
        if tp is Any:
            return "Any"
        if tp is type(None):
            return "None"
        if isinstance(tp, UnionType):
            parts = [self.render(arg) for arg in get_args(tp)]
            return " | ".join(dict.fromkeys(parts))
        origin = get_origin(tp)
        if origin is Annotated:
            return self.render(get_args(tp)[0])
        if origin is Literal:
            self._typing_imports.add("Literal")
            values = ", ".join(repr(value) for value in get_args(tp))
            return f"Literal[{values}]"
        if origin in (list, set, frozenset):
            args = get_args(tp) or (Any,)
            if origin is set:
                container = "set"
            elif origin is frozenset:
                container = "frozenset"
            else:
                container = "list"
            return f"{container}[{self.render(args[0])}]"
        if origin is tuple:
            args = get_args(tp)
            if len(args) == 2 and args[1] is Ellipsis:
                return f"tuple[{self.render(args[0])}, ...]"
            return f"tuple[{', '.join(self.render(arg) for arg in args)}]"
        if origin is dict:
            key, value = get_args(tp) or (Any, Any)
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
                self._module_imports.setdefault("datetime", set()).add(tp.__name__)
                return tp.__name__
            self._module_imports.setdefault(tp.__module__, set()).add(tp.__qualname__.split(".")[0])
            return tp.__qualname__
        return repr(tp)

    def build_imports(self) -> Mapping[str, set[str]]:
        return self._module_imports

    @property
    def typing_names(self) -> set[str]:
        return set(self._typing_imports)

    def require_typing(self, name: str) -> None:
        self._typing_imports.add(name)
