import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from functools import cache
from typing import Any, get_type_hints, is_typeddict

from dclassql.model_inspector import TypeHint


def serialize_json_value(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(_to_json_value(value), ensure_ascii=False, separators=(",", ":"))


def deserialize_json_value(value: object, annotation: Any) -> object:
    if value is None:
        return None
    if isinstance(value, bytes):
        text = value.decode()
    elif isinstance(value, str):
        text = value
    else:
        raise TypeError(f"JSON column value must be str or bytes, got {type(value)!r}")
    return _from_json_value(json.loads(text), TypeHint(annotation))


def _to_json_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _to_json_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _to_json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_to_json_value(item) for item in value]
    if isinstance(value, set | frozenset):
        raise TypeError("set and frozenset are not supported in JSON values")
    return value


def _from_json_value(value: object, type_hint: TypeHint) -> object:
    if value is None:
        return None
    type_hint = type_hint.without_transparent_wrappers()
    annotation = type_hint.source
    origin = type_hint.origin
    args = type_hint.args
    if origin is list:
        item_hint = TypeHint(args[0]) if args else TypeHint(Any)
        if not isinstance(value, list):
            raise TypeError(f"Expected JSON list for {annotation!r}")
        return [_from_json_value(item, item_hint) for item in value]
    if origin is tuple:
        if not isinstance(value, list):
            raise TypeError(f"Expected JSON list for {annotation!r}")
        if len(args) == 2 and args[1] is Ellipsis:
            item_hint = TypeHint(args[0])
            return tuple(_from_json_value(item, item_hint) for item in value)
        item_hints = tuple(TypeHint(arg) for arg in args)
        if len(value) != len(item_hints):
            raise TypeError(
                f"Expected {len(item_hints)} JSON items for {annotation!r}, "
                f"got {len(value)}"
            )
        return tuple(
            _from_json_value(item, child)
            for item, child in zip(value, item_hints)
        )
    if origin is dict:
        value_hint = TypeHint(args[1])
        if not isinstance(value, dict):
            raise TypeError(f"Expected JSON object for {annotation!r}")
        return {key: _from_json_value(item, value_hint) for key, item in value.items()}
    if annotation is datetime:
        if not isinstance(value, str):
            raise TypeError("datetime JSON value must be a string")
        return datetime.fromisoformat(value)
    if annotation is date:
        if not isinstance(value, str):
            raise TypeError("date JSON value must be a string")
        return date.fromisoformat(value)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation(value)
    if isinstance(annotation, type) and is_typeddict(annotation):
        if not isinstance(value, dict):
            raise TypeError(f"Expected JSON object for {annotation.__name__}")
        return {
            name: _from_json_value(value[name], field_hint)
            for name, field_hint in _field_hints(annotation).items()
            if name in value
        }
    if isinstance(annotation, type) and is_dataclass(annotation):
        if not isinstance(value, dict):
            raise TypeError(f"Expected JSON object for {annotation.__name__}")
        field_hints = _field_hints(annotation)
        payload: dict[str, object] = {}
        for field in fields(annotation):
            if field.name not in value:
                continue
            field_hint = field_hints.get(field.name)
            if field_hint is None:
                field_hint = TypeHint(field.type)
            payload[field.name] = _from_json_value(value[field.name], field_hint)
        return annotation(**payload)
    return value


@cache
def _field_hints(annotation: type[Any]) -> dict[str, TypeHint]:
    return {
        name: TypeHint(field_annotation)
        for name, field_annotation in get_type_hints(annotation).items()
    }
