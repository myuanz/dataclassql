from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from types import UnionType
from typing import Annotated, Any, Iterable, Mapping, Sequence, get_args, get_origin, Literal

from .model_inspector import ColumnInfo, ForeignKeyInfo, ModelInfo, inspect_models, DataSourceConfig


@dataclass(slots=True)
class GeneratedModule:
    code: str
    model_names: tuple[str, ...]


def generate_client(models: Sequence[type[Any]]) -> GeneratedModule:
    model_infos = inspect_models(models)
    renderer = _TypeRenderer({info.model: name for name, info in model_infos.items()})

    header_lines: list[str] = ["from __future__ import annotations", ""]

    base_imports = {"from dataclasses import dataclass, field"}

    model_imports: dict[str, set[str]] = defaultdict(set)
    for info in model_infos.values():
        module = info.model.__module__
        model_imports.setdefault(module, set()).add(info.model.__name__)

    body_sections: list[str] = [_render_metadata_base()]

    rendered_models: list[str] = []
    for name in sorted(model_infos.keys()):
        info = model_infos[name]
        rendered_models.append(_render_model(info, renderer))

    body_sections.extend(rendered_models)
    body_sections.append(_render_client_class(model_infos))

    module_imports = renderer.build_imports()
    combined_imports: dict[str, set[str]] = defaultdict(set)
    for module, names in model_imports.items():
        combined_imports[module].update(names)
    for module, names in module_imports.items():
        combined_imports[module].update(names)

    import_lines = sorted(base_imports)
    for module, names in sorted(combined_imports.items()):
        names_list = ", ".join(sorted(names))
        import_lines.append(f"from {module} import {names_list}")
    typing_names = {"Any", "Literal", "Mapping", "Sequence", "TypedDict"}
    typing_names.update(renderer.typing_names)
    if typing_names:
        import_lines.append(f"from typing import {', '.join(sorted(typing_names))}")

    lines = header_lines + import_lines + [""]
    for section in body_sections:
        lines.append(section)
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    if lines and lines[-1] != "":
        lines.append("")
    lines.append(_render_all(model_infos))
    code = "\n".join(lines) + "\n"
    return GeneratedModule(code=code, model_names=tuple(sorted(model_infos.keys())))


def _render_metadata_base() -> str:
    return """@dataclass(slots=True)
class DataSourceConfig:
    provider: str
    url: str | None
    name: str | None = None

    @property
    def key(self) -> str:
        return self.name or self.provider


@dataclass(slots=True)
class ForeignKeySpec:
    local_columns: tuple[str, ...]
    remote_model: type[Any]
    remote_columns: tuple[str, ...]
    backref: str | None
"""


def _render_model(info: ModelInfo, renderer: "_TypeRenderer") -> str:
    alias_block, include_alias, sortable_alias = _render_type_aliases(info)
    sections: list[str] = []
    if alias_block:
        sections.append(alias_block)
    sections.append(_render_insert_structures(info, renderer))
    sections.append(_render_where_dict(info, renderer))
    sections.append(_render_table_class(info, renderer, include_alias, sortable_alias))
    return "\n\n".join(sections)


def _render_type_aliases(info: ModelInfo) -> tuple[str, str, str]:
    name = info.model.__name__
    include_literals = sorted({relation.target.__name__ for relation in info.relations})
    sortable_literals = [col.name for col in info.columns]

    lines: list[str] = []
    include_alias = f"T{name}IncludeCol"
    include_literal_expr = _literal_expression(include_literals)
    lines.append(f"{include_alias} = {include_literal_expr}")

    sortable_alias = f"T{name}SortableCol"
    lines.append(f"{sortable_alias} = {_literal_expression(sortable_literals)}")

    return "\n".join(lines), include_alias, sortable_alias


def _render_insert_structures(info: ModelInfo, renderer: "_TypeRenderer") -> str:
    insert_fields = info.columns
    dataclass_lines: list[str] = []
    for col in insert_fields:
        annotation = _format_insert_annotation(col, renderer)
        default_fragment = _render_default_fragment(info.model.__name__, col)
        if default_fragment is not None:
            dataclass_lines.append(f"    {col.name}: {annotation} = {default_fragment}")
        else:
            dataclass_lines.append(f"    {col.name}: {annotation}")
    if not dataclass_lines:
        dataclass_lines.append("    pass")
    dataclass_block = f"@dataclass(slots=True)\nclass {info.model.__name__}Insert:\n" + "\n".join(dataclass_lines)

    dict_lines = []
    for col in insert_fields:
        annotation = _format_insert_annotation(col, renderer)
        dict_lines.append(f"    {col.name}: {annotation}")
    if not dict_lines:
        dict_lines.append("    pass")
    dict_block = f"class {info.model.__name__}InsertDict(TypedDict):\n" + "\n".join(dict_lines)

    return "\n\n".join([dataclass_block, dict_block])


def _render_where_dict(info: ModelInfo, renderer: "_TypeRenderer") -> str:
    name = info.model.__name__
    lines = [f"class {name}WhereDict(TypedDict, total=False):"]
    field_lines: list[str] = []
    for col in info.columns:
        annotation = renderer.render(col.python_type)
        if "None" not in annotation:
            annotation = f"{annotation} | None"
        field_lines.append(f"    {col.name}: {annotation}")
    if not field_lines:
        field_lines.append("    pass")
    lines.extend(field_lines)
    return "\n".join(lines)


def _render_table_class(info: ModelInfo, renderer: "_TypeRenderer", include_alias: str | None, sortable_alias: str) -> str:
    name = info.model.__name__
    indent = "    "
    lines = [f"class {name}Table:"]
    lines.append(f"{indent}model = {name}")
    lines.append(f"{indent}insert_model = {name}Insert")
    ds = info.datasource
    ds_url_repr = repr(ds.url)
    lines.append(
        f"{indent}datasource = DataSourceConfig(provider={ds.provider!r}, url={ds_url_repr}, name={repr(ds.name)})"
    )
    lines.append(f"{indent}columns = {_tuple_literal(col.name for col in info.columns)}")
    lines.append(f"{indent}primary_key = {_tuple_literal(info.primary_key)}")
    if info.indexes:
        lines.append(f"{indent}indexes = {_tuple_literal(tuple(idx) for idx in info.indexes)}")
    else:
        lines.append(f"{indent}indexes = ()")
    if info.unique_indexes:
        lines.append(f"{indent}unique_indexes = {_tuple_literal(tuple(idx) for idx in info.unique_indexes)}")
    else:
        lines.append(f"{indent}unique_indexes = ()")
    if info.foreign_keys:
        lines.append(f"{indent}foreign_keys = (")
        for fk in info.foreign_keys:
            lines.append(f"{indent*2}ForeignKeySpec(")
            lines.append(f"{indent*3}local_columns={_tuple_literal(fk.local_columns)},")
            lines.append(f"{indent*3}remote_model={fk.remote_model.__name__},")
            lines.append(f"{indent*3}remote_columns={_tuple_literal(fk.remote_columns)},")
            lines.append(f"{indent*3}backref={repr(fk.backref_attribute)},")
            lines.append(f"{indent*2}),")
        lines.append(f"{indent})")
    else:
        lines.append(f"{indent}foreign_keys = ()")
    lines.append("")
    lines.append(f"{indent}def __init__(self, backend: Any) -> None:")
    lines.append(f"{indent*2}self._backend = backend")
    lines.append("")
    lines.append(f"{indent}def insert(self, data: {name}Insert | {name}InsertDict) -> {name}:")
    lines.append(f"{indent*2}raise NotImplementedError('Database insert is not implemented yet')")
    lines.append("")
    lines.append(f"{indent}def insert_many(self, data: Sequence[{name}Insert | {name}InsertDict]) -> list[{name}]:")
    lines.append(f"{indent*2}raise NotImplementedError('Database insert_many is not implemented yet')")
    lines.append("")
    include_annotation = f"dict[{include_alias}, bool] | None"
    lines.append(f"{indent}def find_many(self, *, where: {name}WhereDict | None = None, include: {include_annotation} = None, order_by: Sequence[tuple[{sortable_alias}, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[{name}]:")
    lines.append(f"{indent*2}raise NotImplementedError('Query generation is not implemented yet')")
    lines.append("")
    lines.append(f"{indent}def find_first(self, *, where: {name}WhereDict | None = None, include: {include_annotation} = None, order_by: Sequence[tuple[{sortable_alias}, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> {name} | None:")
    lines.append(f"{indent*2}raise NotImplementedError('Query generation is not implemented yet')")
    return "\n".join(lines)


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


def _render_client_class(model_infos: Mapping[str, ModelInfo]) -> str:
    indent = "    "
    lines = ["class GeneratedClient:"]
    datasource_configs: dict[str, DataSourceConfig] = {}
    for info in model_infos.values():
        datasource = info.datasource
        key = datasource.name or datasource.provider
        existing = datasource_configs.get(key)
        if existing is None:
            datasource_configs[key] = datasource
        elif existing != datasource:
            raise ValueError(
                f"Conflicting datasource key '{key}' for providers"
            )
    lines.append(f"{indent}datasources = {{")
    for key in sorted(datasource_configs.keys()):
        ds = datasource_configs[key]
        lines.append(
            f"{indent*2}{key!r}: DataSourceConfig(provider={ds.provider!r}, url={repr(ds.url)}, name={repr(ds.name)}),"
        )
    lines.append(f"{indent}}}")
    lines.append("")
    lines.append(f"{indent}def __init__(self, connections: Mapping[str, Any]) -> None:")
    lines.append(f"{indent*2}self._connections = connections")
    lines.append(f"{indent*2}for key in self.datasources.keys():")
    lines.append(f"{indent*3}if key not in connections:")
    lines.append(f"{indent*4}raise KeyError(f'datasource {{key}} missing connection')")
    for name in sorted(model_infos.keys()):
        attr = _camel_to_snake(name)
        datasource = model_infos[name].datasource
        ds_key = datasource.name or datasource.provider
        lines.append(f"{indent*2}self.{attr} = {name}Table(connections[{ds_key!r}])")
    return "\n".join(lines)


def _render_all(model_infos: Mapping[str, ModelInfo]) -> str:
    exports: list[str] = ["DataSourceConfig", "ForeignKeySpec", "GeneratedClient"]
    for name in sorted(model_infos.keys()):
        exports.extend([
            f"T{name}IncludeCol",
            f"T{name}SortableCol",
            f"{name}Insert",
            f"{name}InsertDict",
            f"{name}WhereDict",
            f"{name}Table",
        ])
    exports_literal = ", ".join(f"\"{item}\"" for item in exports)
    return f"__all__ = ({exports_literal},)"


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


def _camel_to_snake(name: str) -> str:
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    return pattern.sub("_", name).lower()


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
