from types import UnionType
from typing import Annotated, Any, Self, TypeAliasType, Union, get_args, get_origin

from .fields import FieldTo


_UNION_TYPES = (UnionType, Union)
_MISSING = object()


class TypeHint:
    r"""Python 类型标注的封装

    +------------------------------------------+------------------+-----------------------------------+
    | source                                   | origin           | args                              |
    +------------------------------------------+------------------+-----------------------------------+
    | int                                      | None             | ()                                |
    | User | None                              | UnionType        | (User, None)                      |
    | Annotated[list[str] | None, "metadata"]  | Annotated        | (list[str] | None, "metadata")    |
    | list[User]                               | list             | (User,)                           |
    +------------------------------------------+------------------+-----------------------------------+

    """

    __slots__ = ("source", "origin", "args")

    source: Any
    '''调用方传入的原始类型。'''

    origin: Any
    '''get_origin(source) 的原始结果。'''

    args: tuple[Any, ...]
    '''get_args(source) 的原始结果。'''

    def __init__(self, source: Any) -> None:
        self.source = source
        self.origin = get_origin(source)
        self.args = get_args(source)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TypeHint):
            return False
        return (
            self.source,
            self.origin,
            self.args,
        ) == (
            other.source,
            other.origin,
            other.args,
        )

    @property
    def is_alias(self) -> bool:
        '''例如 `type A = list[int]` 中的 `a`'''
        return isinstance(self.source, TypeAliasType)

    @property
    def is_annotated(self) -> bool:
        '''例如 `a: Annotated[list[int], 'asd']` 中的类型标注'''
        return self.origin is Annotated

    @property
    def has_optional_wrapper(self) -> bool:
        type_hint = self
        while type_hint.is_alias or type_hint.is_annotated:
            type_hint = type_hint._transparent_inner()
        return type_hint._without_optional() is not type_hint.source

    def without_transparent_wrappers(self) -> Self:
        '''去除Annotate包装，再去除 `Optional` 和 `|None`'''
        type_hint = self
        while True:
            if type_hint.is_alias or type_hint.is_annotated:
                type_hint = type_hint._transparent_inner()
                continue
            optional_value = type_hint._without_optional()
            if optional_value is type_hint.source:
                return type_hint
            type_hint = self.__class__(optional_value)

    def _transparent_inner(self) -> Self:
        if self.is_alias:
            return self.__class__(self.source.__value__)
        if self.is_annotated:
            return self.__class__(self.args[0])
        raise TypeError(f"{self.source!r} is not a transparent type wrapper")


    def _without_optional(self) -> Any:
        if self.origin not in _UNION_TYPES:
            return self.source
        non_none = tuple(arg for arg in self.args if arg is not type(None))
        if len(self.args) == 2 and len(non_none) == 1:
            return non_none[0]
        return self.source


type FieldToTypeHint = FieldTo[TypeHint]
