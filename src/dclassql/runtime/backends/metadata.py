from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(slots=True)
class ColumnSpec:
    name: str
    optional: bool
    auto_increment: bool
    has_default: bool
    has_default_factory: bool


@dataclass(slots=True)
class RelationSpec:
    name: str
    table_name: str
    table_module: str
    many: bool
    mapping: tuple[tuple[str, str], ...]
    table_factory: Callable[[], type[Any]] | None = None


@dataclass(slots=True)
class ForeignKeySpec:
    local_columns: tuple[str, ...]
    remote_model: type[Any]
    remote_columns: tuple[str, ...]
    backref: str | None
