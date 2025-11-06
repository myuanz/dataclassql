from __future__ import annotations

import sqlite3
from collections.abc import Sequence as ABCSequence
from typing import Any, TypeVar

from dclassql.table_spec import Col

T = TypeVar("T")


def ensure_col_sequence(value: Col | tuple[Col, ...]) -> list[Col]:
    """Normalize a column or tuple of columns into a list."""
    if isinstance(value, Col):
        return [value]
    return list(value)


def ensure_sequence(value: object, *, label: str) -> list[object]:
    """Ensure the given value is a non-string sequence and return it as a list."""
    if isinstance(value, ABCSequence) and not isinstance(value, (str, bytes)):
        return list(value)
    raise TypeError(f"{label} expects a sequence of values")


def ensure_string(value: object, *, operator: str) -> str:
    """Ensure operator operands are strings."""
    if isinstance(value, str):
        return value
    raise TypeError(f"{operator} expects a string operand")


def ensure_sqlite_row_factory(connection: sqlite3.Connection) -> None:
    """Guarantee sqlite rows are accessible by column name."""
    if connection.row_factory is None:
        connection.row_factory = sqlite3.Row
