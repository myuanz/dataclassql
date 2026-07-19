import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, get_type_hints

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
    return _from_json_value(json.loads(text), annotation)


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
    return value


def _from_json_value(value: object, annotation: Any) -> object:
    type_hint = TypeHint.parse(annotation).without_optional()
    annotation = type_hint.annotation
    origin = type_hint.origin
    args = type_hint.args
    if origin is list:
        item_type = args[0] if args else Any
        if not isinstance(value, list):
            raise TypeError(f"Expected JSON list for {annotation!r}")
        return [_from_json_value(item, item_type) for item in value]
    if origin is tuple:
        if not isinstance(value, list):
            raise TypeError(f"Expected JSON list for {annotation!r}")
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_from_json_value(item, args[0]) for item in value)
        return tuple(_from_json_value(item, item_type) for item, item_type in zip(value, args))
    if origin is dict:
        value_type = args[1] if len(args) == 2 else Any
        if not isinstance(value, dict):
            raise TypeError(f"Expected JSON object for {annotation!r}")
        return {key: _from_json_value(item, value_type) for key, item in value.items()}
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
    if isinstance(annotation, type) and is_dataclass(annotation):
        if not isinstance(value, dict):
            raise TypeError(f"Expected JSON object for {annotation.__name__}")
        hints = get_type_hints(annotation)
        payload = {
            field.name: _from_json_value(value[field.name], hints.get(field.name, field.type))
            for field in fields(annotation)
            if field.name in value
        }
        return annotation(**payload)
    return value
