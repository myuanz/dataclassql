from __future__ import annotations

import sys
from dataclasses import MISSING, dataclass, fields, is_dataclass
from types import UnionType
from urllib.parse import urlparse
from typing import Annotated, Any, Iterable, Literal, Mapping, Sequence, get_args, get_origin, get_type_hints

from .table_spec import Col, TableInfo
from .utils.ensure import ensure_col_sequence


@dataclass(slots=True)
class ColumnInfo:
    name: str
    python_type: Any
    optional: bool
    auto_increment: bool
    storage_kind: Literal["scalar", "json"]
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
    relation_attribute: str | None
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


@dataclass(slots=True, frozen=True)
class DataSourceConfig:
    url: str
    name: str | None = None

    @property
    def provider(self) -> str:
        parsed = urlparse(self.url)
        if not parsed.scheme:
            raise ValueError(f"Datasource url must include provider scheme: {self.url!r}")
        return parsed.scheme

    @property
    def identity(self) -> str:
        return self.url


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
    def __init__(self, name: str, table: type[Any], relation_attribute: str | None = None) -> None:
        super().__init__(name, table)
        self.relation_attribute = relation_attribute

    def __eq__(self, other: object) -> ForeignKeyComparison | bool: # type: ignore[override]
        other_col = _normalize_col(other)
        if other_col is None:
            return NotImplemented  # type: ignore[return-value]
        return ForeignKeyComparison(self, other_col)

    def _to_base(self) -> Col:
        return Col(self.name, table=self.table)


class RelationProxy:
    def __init__(self, target: type[Any], attribute: str) -> None:
        self._target = target
        self._attribute = attribute

    def __getattr__(self, name: str) -> _ProxyCol:
        return _ProxyCol(name, table=self._target, relation_attribute=self._attribute)


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
            return RelationProxy(spec.target, name)
        raise AttributeError(name)


def _validate_model_supports_weakref(model: type[Any]) -> None:
    if not hasattr(model, "__slots__"):
        return
    if hasattr(model, "__weakref__"):
        return
    raise TypeError(
        f"Model {model.__name__} uses slots=True but does not support weak references. "
        f"Use @dataclass(..., slots=True, weakref_slot=True)."
    )

def inspect_models(models: Sequence[type[Any]]) -> dict[str, ModelInfo]:
    for model in models:
        _validate_model_supports_weakref(model)

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
        # 第一遍找出所有模型和关系候选，第二遍把有 dataclass 对象但没有任何关系的字段变成 JSON

        # round 1
        foreign_key_map: dict[type[Any], list[ForeignKeyInfo]] = {}
        relation_name_map: dict[type[Any], set[str]] = {model: set() for model in models}
        for model in models:
            foreign_keys, local_relation_names, remote_relation_names = _extract_foreign_keys(
                model,
                field_specs_map[model],
            )
            foreign_key_map[model] = foreign_keys
            relation_name_map[model].update(local_relation_names)
            for remote_model, attr in remote_relation_names:
                if remote_model in relation_name_map:
                    relation_name_map[remote_model].add(attr)

        # round 2
        infos: dict[str, ModelInfo] = {}
        for model in models:
            annotations = annotations_map[model]
            columns, relations, specs = _categorize_fields(
                model,
                annotations,
                registry,
                relation_names=relation_name_map[model],
            )
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
            infos[model.__name__] = ModelInfo(
                model=model,
                columns=columns,
                relations=relations,
                primary_key=primary_key,
                indexes=indexes,
                unique_indexes=unique_indexes,
                foreign_keys=foreign_key_map[model],
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
    relation_names: set[str] | None = None,
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
        if (relation_names is None or name in relation_names) and _is_relationship(base_annotation, registry):
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
                storage_kind="json" if _is_json_dataclass(base_annotation, registry) else "scalar",
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
    if isinstance(tp, type) and tp in registry.values():
        return True
    if isinstance(tp, str) and tp in registry:
        return True
    return False


def _is_json_dataclass(tp: Any, registry: Mapping[str, type[Any]]) -> bool:
    return isinstance(tp, type) and is_dataclass(tp)


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
        return value
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


def _extract_foreign_keys(
    model: type[Any],
    specs: Mapping[str, FieldSpec],
) -> tuple[list[ForeignKeyInfo], set[str], list[tuple[type[Any], str]]]:
    if not hasattr(model, "foreign_key"):
        return [], set(), []
    fake = FakeSelf(model, specs)
    fn = getattr(model, "foreign_key")
    results = fn(fake)
    entries = _iterate_results(results)
    foreign_keys: list[ForeignKeyInfo] = []
    local_relation_names: set[str] = set()
    remote_relation_names: list[tuple[type[Any], str]] = []
    for entry in entries:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise TypeError("foreign_key must yield tuples of (comparison, backref)")
        comparison, backref = entry
        if not isinstance(comparison, ForeignKeyComparison):
            raise TypeError("foreign_key comparison must be column equality")
        local_cols, remote_cols = _determine_direction(model, comparison)
        backref_attr: str | None = None
        remote_model = remote_cols[0].table
        relation_attr = _relation_attribute_from_cols(remote_cols)
        if relation_attr is not None:
            local_relation_names.add(relation_attr)
        if backref is not None and not isinstance(backref, RelationAttribute):
            raise TypeError("foreign_key backref must be a relation attribute or None")
        if isinstance(backref, RelationAttribute):
            backref_attr = backref.attribute
            remote_model = backref.model
            remote_relation_names.append((remote_model, backref_attr))
        foreign_keys.append(
            ForeignKeyInfo(
                local_columns=tuple(col.name for col in local_cols),
                remote_model=remote_model,
                remote_columns=tuple(col.name for col in remote_cols),
                relation_attribute=relation_attr,
                backref_attribute=backref_attr,
            )
        )
    return foreign_keys, local_relation_names, remote_relation_names


def _relation_attribute_from_cols(cols: Sequence[Col]) -> str | None:
    attrs = {
        getattr(col, "relation_attribute", None)
        for col in cols
        if isinstance(col, _ProxyCol) and getattr(col, "relation_attribute", None) is not None
    }
    if len(attrs) > 1:
        raise ValueError("Composite foreign key cannot mix multiple relation attributes")
    return next(iter(attrs), None)


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
            "{'url': 'sqlite:///example.db'}"
        )
    if "url" not in config:
        raise ValueError(
            f"Module {module.__name__} __datasource__ must declare a 'url' key"
        )
    url = str(config["url"])
    raw_name = config.get("name")
    name = str(raw_name) if raw_name is not None else None
    return DataSourceConfig(url=url, name=name)
