from __future__ import annotations

from typing import Any

from dclassql.codegen import ClientCompiler
from dclassql.model_inspector import ModelGraph, inspect_models

from .test_codegen import Address, BirthDay, Book, User, UserBook


def _compiler(models: list[type[Any]]) -> ClientCompiler:
    return ClientCompiler.from_models(models, client_class_name="UserModelClient")


def test_model_graph_indexes_models_and_keeps_inspect_models_compatible() -> None:
    models = [User, Address, BirthDay, Book, UserBook]
    graph = ModelGraph.from_models(models)

    assert set(graph.by_name) == {model.__name__ for model in models}
    assert all(graph.by_model[model] is graph.by_name[model.__name__] for model in models)
    assert inspect_models(models) == graph.by_name


def test_model_context_shapes_insert_and_typeddict_sections() -> None:
    compiler = _compiler([User, Address, BirthDay, Book, UserBook])

    address_info = compiler.graph.by_name["Address"]
    address_ctx = compiler.build_model_context(address_info)

    assert address_ctx.name == "Address"
    assert address_ctx.model_info is address_info

    id_field = next(field for field in address_ctx.insert_fields if field.name == "id")
    assert id_field.default_expr == "None"

    typed_id_field = next(field for field in address_ctx.typed_dict_fields if field.name == "id")
    assert typed_id_field.annotation.startswith("NotRequired[")

    where_names = {field.name for field in address_ctx.where_fields}
    assert {"id", "location", "user_id", "AND", "OR", "NOT", "user"} <= where_names

    assert any(spec.auto_increment for spec in address_ctx.column_specs)
    assert "NotRequired" in compiler.renderer.typing_names
    relation_names = sorted(
        relationship.local.attribute
        for relationship in compiler.graph.relationships.by_model(Address)
    )
    assert relation_names == ["user"]
    relation_filter_names = [flt.name for flt in address_ctx.relation_filters]
    assert relation_filter_names == ["AddressUserRelationFilter"]
    column_names = [col.name for col in address_ctx.model_info.columns]
    assert column_names == ["id", "location", "user_id"]


def test_client_context_binds_models_to_datasource_backends() -> None:
    compiler = _compiler([User, Address, BirthDay, Book, UserBook])

    client_ctx = compiler.build_client_context()

    assert client_ctx.class_name == "UserModelClient"
    assert client_ctx.datasource.url_repr == "'sqlite:///analytics.db'"

    user_binding = next(binding for binding in client_ctx.model_bindings if binding.model_name == "User")
    assert user_binding.attr_name == "user"
    assert len(client_ctx.model_bindings) == len(compiler.graph.by_name)


def test_collect_exports_includes_expected_symbols() -> None:
    compiler = _compiler([User, Address, BirthDay])
    contexts = [
        compiler.build_model_context(compiler.graph.by_name[name])
        for name in sorted(compiler.graph.by_name)
    ]

    exports = compiler.collect_exports(contexts)

    assert "UserModelClient" in exports
    assert "DataSourceConfig" in exports
    assert "TableRelation" in exports
    for name in ("User", "Address", "BirthDay"):
        assert f"{name}Table" in exports
        assert f"T{name}IncludeCol" in exports
        assert f"{name}IncludeDict" in exports
        assert f"{name}OrderByDict" in exports
    assert "UserAddressesRelationFilter" in exports
