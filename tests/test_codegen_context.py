from __future__ import annotations

from typing import Any

from dclassql.codegen import (
    _ScalarFilterRegistry,
    _TypeRenderer,
    _build_client_context,
    _build_model_context,
    _collect_exports,
)
from dclassql.model_inspector import inspect_models

from .test_codegen import Address, BirthDay, Book, User, UserBook


def _prepare_context(models: list[type[Any]]):
    model_infos = inspect_models(models)
    renderer = _TypeRenderer({info.model: name for name, info in model_infos.items()})
    filter_registry = _ScalarFilterRegistry(renderer)
    return model_infos, renderer, filter_registry


def test_model_context_shapes_insert_and_typeddict_sections() -> None:
    model_infos, renderer, filter_registry = _prepare_context([User, Address, BirthDay, Book, UserBook])

    address_ctx = _build_model_context(model_infos["Address"], renderer, model_infos, filter_registry)

    assert address_ctx.name == "Address"
    assert address_ctx.model_info is model_infos["Address"]

    id_field = next(field for field in address_ctx.insert_fields if field.name == "id")
    assert id_field.default_expr == "None"

    typed_id_field = next(field for field in address_ctx.typed_dict_fields if field.name == "id")
    assert typed_id_field.annotation.startswith("NotRequired[")

    where_names = {field.name for field in address_ctx.where_fields}
    assert {"id", "location", "user_id", "AND", "OR", "NOT", "user"} <= where_names

    assert any(spec.auto_increment for spec in address_ctx.column_specs)
    assert "NotRequired" in renderer.typing_names
    relation_names = sorted(rel.name for rel in address_ctx.model_info.relations)
    assert relation_names == ["user"]
    relation_filter_names = [flt.name for flt in address_ctx.relation_filters]
    assert relation_filter_names == ["AddressUserRelationFilter"]
    column_names = [col.name for col in address_ctx.model_info.columns]
    assert column_names == ["id", "location", "user_id"]


def test_client_context_binds_models_to_datasource_backends() -> None:
    model_infos, _, _ = _prepare_context([User, Address, BirthDay, Book, UserBook])

    client_ctx = _build_client_context(model_infos)

    datasource_keys = [item.key for item in client_ctx.datasource_items]
    assert datasource_keys == ["sqlite"]

    backend_method = client_ctx.backend_methods[0]
    assert backend_method.method_name == "_backend_sqlite"

    user_binding = next(binding for binding in client_ctx.model_bindings if binding.model_name == "User")
    assert user_binding.attr_name == "user"
    assert user_binding.backend_method == backend_method.method_name
    assert user_binding.backend_method.startswith("_backend_")
    assert len(client_ctx.model_bindings) == len(model_infos)


def test_collect_exports_includes_expected_symbols() -> None:
    model_infos, renderer, registry = _prepare_context([User, Address, BirthDay])
    contexts = [
        _build_model_context(model_infos[name], renderer, model_infos, registry)
        for name in sorted(model_infos.keys())
    ]

    exports = _collect_exports(contexts)

    assert "Client" in exports
    assert "DataSourceConfig" in exports
    assert "ForeignKeySpec" in exports
    for name in ("User", "Address", "BirthDay"):
        assert f"{name}Table" in exports
        assert f"T{name}IncludeCol" in exports
        assert f"{name}IncludeDict" in exports
        assert f"{name}OrderByDict" in exports
    assert "UserAddressesRelationFilter" in exports
