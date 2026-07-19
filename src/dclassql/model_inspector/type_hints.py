from dataclasses import dataclass, is_dataclass
from datetime import date, datetime
from enum import Enum
from functools import reduce
from operator import or_
from types import UnionType
from typing import Annotated, Any, Self, Union, get_args, get_origin

from .fields import FieldTo


_COLLECTION_TYPES = (list, set, frozenset, tuple)
_UNION_TYPES = (UnionType, Union)


@dataclass(slots=True, frozen=True)
class TypeHint:
    r"""解析后的类型信息。

    | source | annotation | origin | args | value_type | without_optional().source |
    | --- | --- | --- | --- | --- | --- |
    | `int` | `int` | `None` | `()` | `int` | `int` |
    | `typing.Optional[int]` | `typing.Optional[int]` | `typing.Union` | `(int, None)` | `int` | `int` |
    | `Annotated[list[User] \| None, ...]` | `list[User] \| None` | `types.UnionType` | `(list[User], None)` | `User` | `list[User]` |
    | `int \| str \| None` | `int \| str \| None` | `types.UnionType` | `(int, str, None)` | `int \| str` | `int \| str` |
    """

    source: Any
    annotation: Any
    origin: Any
    args: tuple[Any, ...]
    value_type: Any
    is_optional: bool

    @property
    def collection_type(self) -> type[Any] | None:
        type_hint = self.without_optional()
        return type_hint.origin if type_hint.origin in _COLLECTION_TYPES else None

    @property
    def is_collection(self) -> bool:
        return self.collection_type is not None

    @property
    def is_dataclass(self) -> bool:
        return isinstance(self.value_type, type) and is_dataclass(self.value_type)

    @property
    def enum_type(self) -> type[Enum] | None:
        type_hint = self.without_optional()
        if type_hint.origin is None and isinstance(type_hint.annotation, type) and issubclass(type_hint.annotation, Enum):
            return type_hint.annotation
        return None

    @property
    def scalar_base(self) -> type[Any] | None:
        type_hint = self.without_optional()
        annotation = type_hint.annotation
        if type_hint.origin is not None or not isinstance(annotation, type):
            return None
        if annotation is bool:
            return bool
        if issubclass(annotation, str):
            return str
        if issubclass(annotation, bytes):
            return bytes
        if issubclass(annotation, datetime):
            return datetime
        if issubclass(annotation, date):
            return date
        if issubclass(annotation, float):
            return float
        if issubclass(annotation, int):
            return int
        return None

    @classmethod
    def parse(cls, source: Any) -> Self:
        annotation = cls._unwrap_alias_and_annotated(source)
        origin = get_origin(annotation)
        args = get_args(annotation)
        non_none = tuple(arg for arg in args if arg is not type(None))
        is_optional = origin in _UNION_TYPES and len(non_none) < len(args)
        if is_optional:
            value_type = cls.parse(cls._join_types(non_none)).value_type
        elif origin in _COLLECTION_TYPES and args:
            value_type = cls._unwrap_alias_and_annotated(args[0])
        else:
            value_type = annotation

        return cls(
            source=source,
            annotation=annotation,
            origin=origin,
            args=args,
            value_type=value_type,
            is_optional=is_optional,
        )

    def without_optional(self) -> Self:
        if not self.is_optional:
            return self
        non_none = tuple(arg for arg in self.args if arg is not type(None))
        return self.parse(self._join_types(non_none))

    @staticmethod
    def _join_types(types: tuple[Any, ...]) -> Any:
        if len(types) == 1:
            return types[0]
        return reduce(or_, types)

    @classmethod
    def _unwrap_alias_and_annotated(cls, annotation: Any) -> Any:
        while True:
            alias_value = getattr(annotation, "__value__", None)
            if alias_value is not None:
                annotation = alias_value
                continue
            if get_origin(annotation) is Annotated:
                annotation = get_args(annotation)[0]
                continue
            return annotation


type FieldToTypeHint = FieldTo[TypeHint]
