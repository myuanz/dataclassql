from __future__ import annotations

import datetime as _dt
import sqlite3
from typing import Final


_REGISTERED: Final[dict[str, bool]] = {"done": False}


def _adapt_date_iso(value: _dt.date) -> str:
    return value.isoformat()


def _adapt_datetime_iso(value: _dt.datetime) -> str:
    return value.isoformat(sep="T")


def _convert_date_iso(raw: bytes) -> _dt.date:
    return _dt.date.fromisoformat(raw.decode())


def _convert_datetime_iso(raw: bytes) -> _dt.datetime:
    text = raw.decode()
    try:
        return _dt.datetime.fromisoformat(text)
    except ValueError:
        return _dt.datetime.fromisoformat(text.replace(" ", "T"))


def register_sqlite_adapters() -> None:
    if _REGISTERED["done"]:
        return

    sqlite3.register_adapter(_dt.date, _adapt_date_iso)
    sqlite3.register_adapter(_dt.datetime, _adapt_datetime_iso)
    sqlite3.register_converter("date", _convert_date_iso)
    sqlite3.register_converter("datetime", _convert_datetime_iso)

    _REGISTERED["done"] = True
