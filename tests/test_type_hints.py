from typing import Annotated, Optional, get_args

from dclassql.model_inspector import TypeHint


def test_type_hint_plain_type() -> None:
    type_hint = TypeHint.parse(int)

    assert type_hint.source is int
    assert type_hint.value is int
    assert not type_hint.is_optional
    assert not type_hint.is_collection
    assert type_hint.collection_type is None


def test_type_hint_optional_collection() -> None:
    source = Annotated[list[str] | None, "metadata"]
    type_hint = TypeHint.parse(source)

    assert type_hint.source == source
    assert type_hint.value is str
    assert type_hint.is_optional
    assert type_hint.is_collection
    assert type_hint.collection_type is list


def test_type_hint_typing_optional_and_multi_union() -> None:
    optional = TypeHint.parse(Optional[int])
    multi = TypeHint.parse(int | str | None)

    assert optional.value is int
    assert optional.is_optional
    assert set(get_args(multi.value)) == {int, str}
    assert multi.is_optional


def test_type_hint_typing_dataclass() -> None:
    from dataclasses import dataclass
    @dataclass
    class MyDataClass:
        x: int
        y: Optional['MyDataClass']

    optional = TypeHint.parse(MyDataClass | None)
    multi = TypeHint.parse(MyDataClass | int | str | None)

    assert optional.value is MyDataClass
    assert optional.is_optional
    assert optional.is_dataclass
    assert set(get_args(multi.value)) == {MyDataClass, int, str}
    assert multi.is_optional
    assert not multi.is_dataclass
