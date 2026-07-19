from dataclasses import dataclass, is_dataclass
from functools import reduce
from operator import or_
from types import UnionType
from typing import Annotated, Any, Self, Union, get_args, get_origin

from .fields import FieldTo


_COLLECTION_TYPES = (list, set, frozenset, tuple)


@dataclass(slots=True, frozen=True)
class TypeHint:
    source: Any
    value: Any
    is_optional: bool
    collection_type: type[Any] | None

    @property
    def is_collection(self) -> bool:
        return self.collection_type is not None

    @property
    def is_dataclass(self) -> bool:
        return isinstance(self.value, type) and is_dataclass(self.value)

    @classmethod
    def parse(cls, source: Any) -> Self:
        current = cls._unwrap_annotated(source)
        origin = get_origin(current)
        args = get_args(current)
        non_none = tuple(arg for arg in args if arg is not type(None))  # noqa: E721
        is_optional = origin in (UnionType, Union) and len(non_none) < len(args)
        if is_optional and non_none:
            current = non_none[0] if len(non_none) == 1 else reduce(or_, non_none)
            current = cls._unwrap_annotated(current)

        origin = get_origin(current)
        collection_type = origin if origin in _COLLECTION_TYPES else None
        if collection_type is not None:
            args = get_args(current)
            if args:
                current = cls._unwrap_annotated(args[0])

        return cls(
            source=source,
            value=current,
            is_optional=is_optional,
            collection_type=collection_type,
        )

    @staticmethod
    def _unwrap_annotated(type_hint: Any) -> Any:
        while get_origin(type_hint) is Annotated:
            type_hint = get_args(type_hint)[0]
        return type_hint


type FieldToTypeHint = FieldTo[TypeHint]
