from __future__ import annotations

import sys
from dataclasses import MISSING, dataclass, fields, is_dataclass
from types import UnionType
from typing import Annotated, Any, Iterable, Mapping, Sequence, get_args, get_origin, get_type_hints

from .table_spec import Col, TableInfo
from .utils.ensure import ensure_col_sequence


@dataclass(slots=True)
class ColumnInfo:
    name: str
    python_type: Any
    optional: bool
    auto_increment: bool
    has_default: bool
    default_value: Any
    has_default_factory: bool
    default_factory: Any | None


@dataclass(slots=True)
class RelationInfo:
    name: str
    target: type[Any]
    many: bool


@dataclass(slots=True)
class ForeignKeyInfo:
    local_columns: tuple[str, ...]
    remote_model: type[Any]
    remote_columns: tuple[str, ...]
    backref_attribute: str | None


@dataclass(slots=True)
class ModelInfo:
    model: type[Any]
    columns: list[ColumnInfo]
    relations: list[RelationInfo]
    primary_key: tuple[str, ...]
    indexes: list[tuple[str, ...]]
    unique_indexes: list[tuple[str, ...]]
    foreign_keys: list[ForeignKeyInfo]
    datasource: 'DataSourceConfig'


@dataclass(slots=True)
class DataSourceConfig:
    provider: str
    url: str | None
    name: str | None = None

    @property
    def key(self) -> str:
        return self.name or self.provider


@dataclass(slots=True)
class FieldSpec:
    name: str
    kind: str
    target: type[Any] | None = None


class RelationAttribute:
    def __init__(self, model: type[Any], attribute: str) -> None:
        self.model = model
        self.attribute = attribute

    def __repr__(self) -> str:  # pragma: no cover - diagnostic only
        return f"RelationAttribute(model={self.model.__name__}, attribute={self.attribute})"


class ForeignKeyComparison:
    def __init__(self, left: Col | tuple[Col, ...], right: Col | tuple[Col, ...]) -> None:
        self.left = left
        self.right = right


class _ProxyCol(Col):
    def __eq__(self, other: object) -> ForeignKeyComparison | bool: # type: ignore[override]
        other_col = _normalize_col(other)
        if other_col is None:
            return NotImplemented  # type: ignore[return-value]
        return ForeignKeyComparison(self._to_base(), other_col)

    def _to_base(self) -> Col:
        return Col(self.name, table=self.table)


class RelationProxy:
    def __init__(self, target: type[Any]) -> None:
        self._target = target

    def __getattr__(self, name: str) -> _ProxyCol:
        return _ProxyCol(name, table=self._target)


class FakeSelf:
    def __init__(self, model: type[Any], specs: Mapping[str, FieldSpec]) -> None:
        self._model = model
        self._specs = specs

    def __getattr__(self, name: str) -> _ProxyCol | RelationProxy:
        spec = self._specs.get(name)
        if spec is None:
            raise AttributeError(name)
        if spec.kind == "column":
            return _ProxyCol(name, table=self._model)
        if spec.kind in {"relation", "relation_many"} and spec.target is not None:
            return RelationProxy(spec.target)
        raise AttributeError(name)


def inspect_models(models: Sequence[type[Any]]) -> dict[str, ModelInfo]:
    registry: dict[str, type[Any]] = {model.__name__: model for model in models}
    globalns: dict[str, Any] = {}
    module_map: dict[type[Any], Any] = {}
    for model in models:
        module = sys.modules.get(model.__module__)
        if module is None:
            module = __import__(model.__module__, fromlist=["*"])
        module_map[model] = module
        globalns.update(vars(module))
    globalns.update(registry)

    annotations_map: dict[type[Any], dict[str, Any]] = {}
    field_specs_map: dict[type[Any], dict[str, FieldSpec]] = {}
    relation_map: dict[type[Any], list[RelationInfo]] = {}
    for model in models:
        annotations = get_type_hints(model, globalns=globalns, include_extras=True)
        annotations_map[model] = annotations
        columns, relations, specs = _categorize_fields(model, annotations, registry)
        field_specs_map[model] = specs
        relation_map[model] = relations

    datasource_map: dict[type[Any], DataSourceConfig] = {
        model: _module_datasource(module_map[model]) for model in models
    }

    backref_records: list[tuple[type[Any], str, Any, bool]] = []
    for model, relations in relation_map.items():
        for relation in relations:
            has_attr = hasattr(model, relation.name)
            previous = getattr(model, relation.name, None)
            setattr(model, relation.name, RelationAttribute(model, relation.name))
            backref_records.append((model, relation.name, previous, has_attr))

    try:
        infos: dict[str, ModelInfo] = {}
        for model in models:
            annotations = annotations_map[model]
            columns, relations, specs = _categorize_fields(model, annotations, registry)
            table_info = TableInfo.from_dc(model)
            primary_key = _col_names(table_info.primary_key.cols)
            indexes: list[tuple[str, ...]] = []
            unique_indexes: list[tuple[str, ...]] = []
            for spec in table_info.index:
                col_names = _col_names(spec.cols)
                if spec.is_unique_index:
                    unique_indexes.append(col_names)
                else:
                    indexes.append(col_names)
            foreign_keys = _extract_foreign_keys(model, field_specs_map[model])
            infos[model.__name__] = ModelInfo(
                model=model,
                columns=columns,
                relations=relations,
                primary_key=primary_key,
                indexes=indexes,
                unique_indexes=unique_indexes,
                foreign_keys=foreign_keys,
                datasource=datasource_map[model],
            )
        return infos
    finally:
        for model, attr, previous, has_attr in backref_records:
            if has_attr:
                setattr(model, attr, previous)
            else:
                delattr(model, attr)


def _categorize_fields(
    model: type[Any],
    annotations: Mapping[str, Any],
    registry: Mapping[str, type[Any]],
) -> tuple[list[ColumnInfo], list[RelationInfo], dict[str, FieldSpec]]:
    columns: list[ColumnInfo] = []
    relations: list[RelationInfo] = []
    specs: dict[str, FieldSpec] = {}

    table_info = TableInfo.from_dc(model)
    pk_cols = set(_col_names(table_info.primary_key.cols))

    for field in fields(model):
        name = field.name
        annotation = annotations.get(name)
        if annotation is None:
            continue
        optional_flag = False
        annotation, optional_flag = _strip_optional(annotation)
        base_annotation = _unwrap_annotation(annotation)
        if _is_relationship(base_annotation, registry):
            target = _resolve_model(base_annotation, registry)
            many = _is_collection_type(annotation)
            relations.append(RelationInfo(name=name, target=target, many=many))
            specs[name] = FieldSpec(name=name, kind="relation_many" if many else "relation", target=target)
            continue
        has_default_value = field.default is not MISSING
        has_default_factory = field.default_factory is not MISSING
        columns.append(
            ColumnInfo(
                name=name,
                python_type=annotations[name],
                optional=optional_flag or has_default_value or has_default_factory,
                auto_increment=_is_auto_increment(name, annotations[name], pk_cols),
                has_default=has_default_value,
                default_value=field.default if has_default_value else None,
                has_default_factory=has_default_factory,
                default_factory=field.default_factory if has_default_factory else None,
            )
        )
        specs[name] = FieldSpec(name=name, kind="column")

    return columns, relations, specs


def _strip_optional(tp: Any) -> tuple[Any, bool]:
    origin = get_origin(tp)
    if origin is UnionType:
        args = get_args(tp)
        non_none = tuple(arg for arg in args if arg is not type(None))  # noqa: E721
        is_optional = len(non_none) < len(args)
        if len(non_none) == 1:
            return non_none[0], is_optional
        return tp, is_optional
    return tp, False


def _unwrap_annotation(tp: Any) -> Any:
    while get_origin(tp) is Annotated:
        tp = get_args(tp)[0]
    if _is_collection_type(tp):
        args = get_args(tp)
        if args:
            return args[0]
    return tp


def _is_collection_type(tp: Any) -> bool:
    origin = get_origin(tp)
    return origin in (list, set, frozenset, tuple)


def _is_relationship(tp: Any, registry: Mapping[str, type[Any]]) -> bool:
    if isinstance(tp, type) and is_dataclass(tp):
        return True
    if isinstance(tp, str) and tp in registry:
        return True
    return False


def _resolve_model(tp: Any, registry: Mapping[str, type[Any]]) -> type[Any]:
    if isinstance(tp, type) and is_dataclass(tp):
        return tp
    if isinstance(tp, str):
        model = registry.get(tp)
        if model is None:
            raise KeyError(f"Unknown model reference: {tp}")
        return model
    raise TypeError(f"Unsupported model type: {tp!r}")


def _is_auto_increment(name: str, annotation: Any, pk_cols: set[str]) -> bool:
    if name != "id" or name not in pk_cols:
        return False
    base, _ = _strip_optional(annotation)
    base = _unwrap_annotation(base)
    return base is int


def _normalize_col(value: object) -> Col | tuple[Col, ...] | None:
    if isinstance(value, _ProxyCol):
        return value._to_base()
    if isinstance(value, Col):
        return value
    if isinstance(value, tuple):
        cols = []
        for item in value:
            col = _normalize_col(item)
            if not isinstance(col, Col):
                return None
            cols.append(col)
        return tuple(cols)
    return None


def _col_names(cols: Col | tuple[Col, ...]) -> tuple[str, ...]:
    if isinstance(cols, Col):
        return (cols.name,)
    return tuple(col.name for col in cols)


def _extract_foreign_keys(model: type[Any], specs: Mapping[str, FieldSpec]) -> list[ForeignKeyInfo]:
    if not hasattr(model, "foreign_key"):
        return []
    fake = FakeSelf(model, specs)
    fn = getattr(model, "foreign_key")
    results = fn(fake)
    entries = _iterate_results(results)
    foreign_keys: list[ForeignKeyInfo] = []
    for entry in entries:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise TypeError("foreign_key must yield tuples of (comparison, backref)")
        comparison, backref = entry
        if not isinstance(comparison, ForeignKeyComparison):
            raise TypeError("foreign_key comparison must be column equality")
        local_cols, remote_cols = _determine_direction(model, comparison)
        backref_attr: str | None = None
        remote_model = remote_cols[0].table
        if isinstance(backref, RelationAttribute):
            backref_attr = backref.attribute
            remote_model = backref.model
        foreign_keys.append(
            ForeignKeyInfo(
                local_columns=tuple(col.name for col in local_cols),
                remote_model=remote_model,
                remote_columns=tuple(col.name for col in remote_cols),
                backref_attribute=backref_attr,
            )
        )
    return foreign_keys


def _iterate_results(results: Any) -> Iterable[Any]:
    if results is None:
        return []
    if isinstance(results, Iterable) and not isinstance(results, (str, bytes)):
        return results
    return [results]


def _determine_direction(model: type[Any], comparison: ForeignKeyComparison) -> tuple[list[Col], list[Col]]:
    left_cols = ensure_col_sequence(comparison.left)
    right_cols = ensure_col_sequence(comparison.right)
    left_local = all(col.table is model for col in left_cols)
    right_local = all(col.table is model for col in right_cols)
    if left_local and not right_local:
        return left_cols, right_cols
    if right_local and not left_local:
        return right_cols, left_cols
    raise ValueError("Unable to determine foreign key direction")
def _module_datasource(module: Any | None) -> DataSourceConfig:
    if module is None:
        raise ValueError("Model module is not available while resolving datasource")
    config = getattr(module, "__datasource__", None)
    if not isinstance(config, Mapping):
        raise ValueError(
            f"Module {module.__name__} must define __datasource__ = "
            "{'provider': 'sqlite', 'url': 'sqlite:///example.db'}"
        )
    if "provider" not in config:
        raise ValueError(
            f"Module {module.__name__} __datasource__ must declare a 'provider' key"
        )
    provider = str(config["provider"])
    raw_url = config.get("url")
    url = str(raw_url) if raw_url is not None else None
    raw_name = config.get("name")
    name = str(raw_name) if raw_name is not None else None
    return DataSourceConfig(provider=provider, url=url, name=name)
