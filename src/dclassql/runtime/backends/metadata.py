from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping


@dataclass(slots=True)
class ColumnSpec:
    '''生成的 client 中需要知道的 Column 规格'''
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
    backref: str | None = None
