from __future__ import annotations

import sys
from dataclasses import MISSING, dataclass, fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from types import UnionType
from typing import (
    Any,
    Literal,
    Mapping,
    Self,
    Sequence,
    Union,
    get_type_hints,
    is_typeddict,
)
from urllib.parse import urlparse

from .relationships import Relationships, inspect_relationships
from .table_constraints import TableConstraints
from .fields import FieldTo
from .type_hints import FieldToTypeHint, TypeHint


_UNION_TYPES = (UnionType, Union)
_REJECTED_COLLECTION_TYPES = (set, frozenset)


@dataclass(slots=True)
class ColumnInfo:
    name: str
    type_hint: TypeHint
    nullable: bool
    auto_increment: bool
    storage_kind: Literal["scalar", "json"]
    scalar_base: type[Any] | None
    enum_type: type[Enum] | None
    has_default: bool
    has_default_factory: bool

    @classmethod
    def from_model(
        cls,
        model: type[Any],
        type_hints: FieldToTypeHint,
        table_constraints: TableConstraints,
        relation_names: set[str],
    ) -> list[Self]:
        columns: list[Self] = []
        primary_key = set(table_constraints.primary_key.names)
        for field in fields(model):
            name = field.name
            type_hint = type_hints.get(name)
            if type_hint is None or name in relation_names:
                continue
            has_default = field.default is not MISSING
            has_default_factory = field.default_factory is not MISSING
            storage_kind, scalar_base, enum_type = cls._analyze_type_hint(
                type_hint,
                inside_json=False,
                seen=set(),
            )
            columns.append(
                cls(
                    name=name,
                    type_hint=type_hint,
                    nullable=type_hint.has_optional_wrapper,
                    auto_increment=(
                        name == "id"
                        and name in primary_key
                        and type_hint.without_transparent_wrappers().source is int
                    ),
                    storage_kind=storage_kind,
                    scalar_base=scalar_base,
                    enum_type=enum_type,
                    has_default=has_default,
                    has_default_factory=has_default_factory,
                )
            )
        return columns

    @classmethod
    def _analyze_type_hint(
        cls,
        type_hint: TypeHint,
        *,
        inside_json: bool,
        seen: set[type[Any]],
    ) -> tuple[
        Literal["scalar", "json"],
        type[Any] | None,
        type[Enum] | None,
    ]:
        type_hint = type_hint.without_transparent_wrappers()
        if type_hint.origin in _UNION_TYPES:
            raise TypeError("Only Optional[T] / T | None unions are supported")
        if type_hint.origin in _REJECTED_COLLECTION_TYPES:
            raise TypeError(f"{type_hint.origin.__name__} annotations are not supported")

        scalar_info = cls._scalar_info(type_hint)
        if scalar_info is not None:
            scalar_base, enum_type = scalar_info
            if inside_json and scalar_base is bytes:
                raise TypeError("bytes is not supported inside JSON values")
            return "scalar", scalar_base, enum_type

        source = type_hint.source
        if isinstance(source, type) and (
            is_dataclass(source) or is_typeddict(source)
        ):
            if source in seen:
                return "json", None, None
            seen.add(source)
            for field_annotation in get_type_hints(source).values():
                cls._analyze_type_hint(
                    TypeHint(field_annotation),
                    inside_json=True,
                    seen=seen,
                )
            return "json", None, None

        if type_hint.origin is list:
            if len(type_hint.args) != 1:
                raise TypeError("JSON list annotation must declare an item type")
            cls._analyze_type_hint(
                TypeHint(type_hint.args[0]),
                inside_json=True,
                seen=seen,
            )
            return "json", None, None

        if type_hint.origin is tuple:
            args = tuple(arg for arg in type_hint.args if arg is not Ellipsis)
            if not args:
                raise TypeError("JSON tuple annotation must declare item types")
            for arg in args:
                cls._analyze_type_hint(
                    TypeHint(arg),
                    inside_json=True,
                    seen=seen,
                )
            return "json", None, None

        if type_hint.origin is dict:
            if len(type_hint.args) != 2:
                raise TypeError("JSON dict annotation must declare key and value types")
            key_hint = TypeHint(type_hint.args[0])
            if key_hint.has_optional_wrapper:
                raise TypeError("JSON dict keys must be str")
            key_hint = key_hint.without_transparent_wrappers()
            if key_hint.source is not str:
                raise TypeError("JSON dict keys must be str")
            cls._analyze_type_hint(
                TypeHint(type_hint.args[1]),
                inside_json=True,
                seen=seen,
            )
            return "json", None, None

        raise TypeError(f"Unsupported column annotation {type_hint.source!r}")

    @staticmethod
    def _scalar_info(
        type_hint: TypeHint,
    ) -> tuple[type[Any] | None, type[Enum] | None] | None:
        source = type_hint.source
        if source is Any or type_hint.origin is Literal:
            return None, None
        if not isinstance(source, type) or type_hint.origin is not None:
            return None
        enum_type = source if issubclass(source, Enum) else None
        if source is bool:
            return bool, enum_type
        if issubclass(source, str):
            return str, enum_type
        if issubclass(source, bytes):
            return bytes, enum_type
        if issubclass(source, datetime):
            return datetime, enum_type
        if issubclass(source, date):
            return date, enum_type
        if issubclass(source, float):
            return float, enum_type
        if issubclass(source, int):
            return int, enum_type
        return (None, enum_type) if enum_type is not None else None


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
class ModelInfo:
    model: type[Any]
    columns: list[ColumnInfo]
    constraints: TableConstraints
    datasource: DataSourceConfig


def _validate_model_supports_weakref(model: type[Any]) -> None:
    if not hasattr(model, "__slots__"):
        return
    if hasattr(model, "__weakref__"):
        return
    raise TypeError(
        f"Model {model.__name__} uses slots=True but does not support weak references. "
        f"Use @dataclass(..., slots=True, weakref_slot=True)."
    )


class ModelGraph:
    def __init__(
        self,
        models: Sequence[type[Any]],
        model_infos: Sequence[ModelInfo],
        relationships: Relationships,
    ) -> None:
        self.models = tuple(models)
        self.by_name = {info.model.__name__: info for info in model_infos}
        self.by_model = {info.model: info for info in model_infos}
        self.relationships = relationships

    @classmethod
    def from_models(cls, models: Sequence[type[Any]]) -> "ModelGraph":
        models = tuple(models)
        for model in models:
            _validate_model_supports_weakref(model)
        # 构建模型注册表，并引入到 globalns
        registry = {model.__name__: model for model in models}
        modules: dict[type[Any], Any] = {}
        globalns: dict[str, Any] = {}
        for model in models:
            module = sys.modules.get(model.__module__)
            if module is None:
                module = __import__(model.__module__, fromlist=["*"])
            modules[model] = module
            globalns.update(vars(module))
        globalns.update(registry)

        # 解析每个 model 里字段的 type_hint
        type_hints_by_model = {
            model: FieldTo.from_mapping(
                {
                    name: TypeHint(source)
                    for name, source in get_type_hints(
                        model,
                        globalns=globalns,
                        include_extras=True,
                    ).items()
                }
            )
            for model in models
        }
        constraints_by_model = {
            model: TableConstraints.from_dc(model) for model in models
        }
        # 然后解析模型的 foreign_key，检查对应的表和字段，获得关系
        relationships = inspect_relationships(
            models,
            type_hints_by_model,
            registry,
        )
        # 所有关系都收集后，才能正确构建 ModelInfo
        model_infos = [
            ModelInfo(
                model=model,
                columns=ColumnInfo.from_model(
                    model,
                    type_hints_by_model[model],
                    constraints_by_model[model],
                    {
                        relationship.local.attribute
                        for relationship in relationships.by_model(model)
                    },
                ),
                constraints=constraints_by_model[model],
                datasource=_module_datasource(modules[model]),
            )
            for model in models
        ]
        graph = cls(models, model_infos, relationships)
        graph._validate_relationships()
        return graph

    def _validate_relationships(self) -> None:
        '''检查一个关系是否被错误共用'''
        endpoints: set[tuple[type[Any], str]] = set()
        for relationship in self.relationships:
            for link in (relationship.local, relationship.remote):
                if link is None:
                    continue
                key = (link.source, link.attribute)
                if key in endpoints:
                    raise ValueError(
                        f"Relation attribute {link.source.__name__}.{link.attribute} "
                        "is used by multiple relationships"
                    )
                endpoints.add(key)

            if relationship.local.many:
                raise TypeError(
                    f"Foreign-key relation {relationship.local.source.__name__}."
                    f"{relationship.local.attribute} must be single-valued"
                )

            local_model = relationship.local.source
            remote_model = relationship.local.target
            for local_column, remote_column in relationship.mapping.items():
                local_type = self._relationship_column_type(
                    local_model,
                    local_column.name,
                )
                remote_type = self._relationship_column_type(
                    remote_model,
                    remote_column.name,
                )
                if local_type != remote_type:
                    raise TypeError(
                        f"Relationship columns {local_model.__name__}.{local_column.name} "
                        f"and {remote_model.__name__}.{remote_column.name} have "
                        f"incompatible types {local_type!r} and {remote_type!r}"
                    )

            local_columns = tuple(column.name for column in relationship.mapping)
            remote_columns = tuple(column.name for column in relationship.mapping.values())
            if not self.by_model[remote_model].constraints.is_unique(remote_columns):
                raise ValueError(
                    f"Relationship target columns {remote_model.__name__}."
                    f"{remote_columns!r} must be a primary key or unique index"
                )

            if (
                relationship.remote is not None
                and not relationship.remote.many
                and not self.by_model[local_model].constraints.is_unique(local_columns)
            ):
                raise ValueError(
                    f"Single-valued backref {relationship.remote.source.__name__}."
                    f"{relationship.remote.attribute} requires unique foreign-key "
                    f"columns {local_model.__name__}.{local_columns!r}"
                )

    def _relationship_column_type(
        self,
        model: type[Any],
        column: str,
    ) -> object:
        column_info = next(
            (item for item in self.by_model[model].columns if item.name == column),
            None,
        )
        if column_info is None:
            raise ValueError(
                f"Relationship column {model.__name__}.{column} does not exist"
            )
        if column_info.storage_kind != "scalar":
            raise TypeError(
                f"Relationship column {model.__name__}.{column} must be scalar"
            )
        type_hint = column_info.type_hint.without_transparent_wrappers()
        return column_info.enum_type or column_info.scalar_base or type_hint.source


def inspect_models(models: Sequence[type[Any]]) -> dict[str, ModelInfo]:
    return ModelGraph.from_models(models).by_name


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
