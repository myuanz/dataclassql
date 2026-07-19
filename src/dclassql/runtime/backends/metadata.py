from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping


@dataclass(slots=True)
class ColumnSpec:
    name: str
    python_type: Any
    storage_kind: Literal["scalar", "json"]
    optional: bool
    auto_increment: bool
    has_default: bool
    has_default_factory: bool


@dataclass(slots=True)
class TableRelation[TTable]:
    attribute: str
    remote_table: Callable[[], type[TTable]]
    many: bool
    mapping: Mapping[str, str]


@dataclass(slots=True)
class ForeignKeySpec:
    local_columns: tuple[str, ...]
    remote_model: type[Any]
    remote_columns: tuple[str, ...]
    backref: str | None
