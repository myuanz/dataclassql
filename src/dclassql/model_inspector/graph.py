from __future__ import annotations

import sys
from dataclasses import MISSING, dataclass, fields
from urllib.parse import urlparse
from typing import Any, Literal, Mapping, Self, Sequence, get_type_hints

from .relationships import Relationships, inspect_relationships
from .table_constraints import TableConstraints
from .fields import FieldTo
from .type_hints import FieldToTypeHint, TypeHint


@dataclass(slots=True)
class ColumnInfo:
    name: str
    type_hint: TypeHint
    optional: bool
    auto_increment: bool
    storage_kind: Literal["scalar", "json"]
    has_default: bool
    default_value: Any
    has_default_factory: bool
    default_factory: Any | None

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
            columns.append(
                cls(
                    name=name,
                    type_hint=type_hint,
                    optional=type_hint.is_optional or has_default or has_default_factory,
                    auto_increment=(
                        name == "id"
                        and name in primary_key
                        and type_hint.value is int
                    ),
                    storage_kind="json" if type_hint.is_dataclass else "scalar",
                    has_default=has_default,
                    default_value=field.default if has_default else None,
                    has_default_factory=has_default_factory,
                    default_factory=(
                        field.default_factory if has_default_factory else None
                    ),
                )
            )
        return columns


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
                    name: TypeHint.parse(source)
                    for name, source in get_type_hints(
                        model,
                        globalns=globalns,
                        include_extras=True,
                    ).items()
                }
            )
            for model in models
        }
        # 然后解析模型的 foreign_key，检查对应的表和字段，获得关系
        relationships = inspect_relationships(
            models,
            type_hints_by_model,
            registry,
        )
        # 所有关系都收集后，才能正确构建 ModelInfo
        model_infos: list[ModelInfo] = []
        for model in models:
            constraints = TableConstraints.from_dc(model)
            relation_names = {
                relationship.local.attribute
                for relationship in relationships.by_model(model)
            }
            model_infos.append(
                ModelInfo(
                    model=model,
                    columns=ColumnInfo.from_model(
                        model,
                        type_hints_by_model[model],
                        constraints,
                        relation_names,
                    ),
                    constraints=constraints,
                    datasource=_module_datasource(modules[model]),
                )
            )
        return cls(models, model_infos, relationships)

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
