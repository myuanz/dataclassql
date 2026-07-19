from types import UnionType
from typing import Annotated, Optional, get_args

from dclassql.model_inspector import TypeHint


type OptionalInt = Optional[int]


def test_type_hint_plain_type() -> None:
    type_hint = TypeHint.parse(int)

    assert type_hint.source is int
    assert type_hint.annotation is int
    assert type_hint.origin is None
    assert type_hint.args == ()
    assert type_hint.value_type is int
    assert not type_hint.is_optional
    assert not type_hint.is_collection
    assert type_hint.collection_type is None


def test_type_hint_optional_collection() -> None:
    source = Annotated[list[str] | None, "metadata"]
    type_hint = TypeHint.parse(source)

    assert type_hint.source == source
    assert type_hint.origin is UnionType
    assert type_hint.args == (list[str], type(None))
    assert type_hint.value_type is str
    assert type_hint.is_optional
    assert type_hint.is_collection
    assert type_hint.collection_type is list
    without_optional = type_hint.without_optional()
    assert without_optional.annotation == list[str]
    assert without_optional.origin is list
    assert without_optional.args == (str,)


def test_type_hint_typing_optional_and_multi_union() -> None:
    optional = TypeHint.parse(Optional[int])
    multi = TypeHint.parse(int | str | None)

    assert optional.value_type is int
    assert optional.is_optional
    assert set(get_args(multi.value_type)) == {int, str}
    assert set(multi.without_optional().args) == {int, str}
    assert multi.is_optional


def test_without_optional_normalizes_alias_annotated_and_optional() -> None:
    assert TypeHint.parse(Optional[int]).without_optional().annotation is int
    assert TypeHint.parse(Annotated[OptionalInt, "metadata"]).without_optional().annotation is int


def test_type_hint_typing_dataclass() -> None:
    from dataclasses import dataclass
    @dataclass
    class MyDataClass:
        x: int
        y: Optional['MyDataClass']

    optional = TypeHint.parse(MyDataClass | None)
    multi = TypeHint.parse(MyDataClass | int | str | None)

    assert optional.value_type is MyDataClass
    assert optional.is_optional
    assert optional.is_dataclass
    assert set(get_args(multi.value_type)) == {MyDataClass, int, str}
    assert multi.is_optional
    assert not multi.is_dataclass
