from types import UnionType
from typing import Annotated, Optional

from dclassql.codegen import TypeHintRenderer
from dclassql.model_inspector import TypeHint


type OptionalInt = Optional[int]
type AnnotatedOptionalList = Annotated[list[str] | None, "metadata"]


def test_type_hint_plain_type() -> None:
    type_hint = TypeHint(int)

    assert type_hint.source is int
    assert type_hint.origin is None
    assert type_hint.args == ()
    assert not type_hint.is_alias
    assert not type_hint.is_annotated
    assert not type_hint.has_optional_wrapper
    assert type_hint.without_transparent_wrappers() is type_hint


def test_type_hint_preserves_annotated_optional_structure() -> None:
    source = Annotated[list[str] | None, "metadata"]
    type_hint = TypeHint(source)

    assert type_hint.source == source
    assert type_hint.origin is Annotated
    assert type_hint.args == (list[str] | None, "metadata")
    assert type_hint.is_annotated
    assert type_hint.has_optional_wrapper

    collection = type_hint.without_transparent_wrappers()
    assert collection.source == list[str]
    assert collection.origin is list
    assert collection.args == (str,)


def test_type_hint_transparent_wrappers_include_pep695_alias() -> None:
    type_hint = TypeHint(AnnotatedOptionalList)

    assert type_hint.source is AnnotatedOptionalList
    assert type_hint.origin is None
    assert type_hint.args == ()
    assert type_hint.is_alias
    assert type_hint.has_optional_wrapper
    assert type_hint.without_transparent_wrappers() == TypeHint(list[str])


def test_type_hint_represents_non_optional_union_and_set_without_policy() -> None:
    union = TypeHint(int | str)
    collection = TypeHint(set[int])

    assert union.origin is UnionType
    assert union.args == (int, str)
    assert not union.has_optional_wrapper
    assert union.without_transparent_wrappers() is union
    assert collection.origin is set
    assert collection.args == (int,)


def test_type_hint_renderer_accepts_type_hint() -> None:
    renderer = TypeHintRenderer({})

    assert renderer.render(TypeHint(Optional[int])) == "int | None"
    assert renderer.render(TypeHint(Annotated[list[str], "metadata"])) == (
        "Annotated[list[str], 'metadata']"
    )
