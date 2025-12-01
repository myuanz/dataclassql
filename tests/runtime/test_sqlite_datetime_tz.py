from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dclassql.codegen import generate_client
from dclassql.push import db_push
from dclassql.runtime.datasource import open_sqlite_connection

__datasource__ = {"provider": "sqlite", "url": None}


@dataclass
class Event:
    id: int
    occurred_at: datetime


def _prepare(url: str, model: type[object]) -> None:
    conn = open_sqlite_connection(url)
    try:
        db_push([model], {"sqlite": conn})
    finally:
        conn.close()


def test_datetime_with_timezone_roundtrip(tmp_path: Path) -> None:
    url = f"sqlite:///{(tmp_path / 'tz.db').as_posix()}"
    globals()["__datasource__"] = {"provider": "sqlite", "url": url}

    _prepare(url, Event)
    module = generate_client([Event])
    ns: dict[str, Any] = {}
    exec(module.code, ns)
    client = ns["Client"]()
    table = client.event
    aware_dt = datetime(2024, 1, 1, 12, 34, 56, 123456, tzinfo=timezone(timedelta(hours=8)))

    inserted = table.insert({"id": 1, "occurred_at": aware_dt})
    fetched = table.find_first(where={"id": 1})

    assert inserted.occurred_at.tzinfo is not None
    assert fetched is not None and fetched.occurred_at.tzinfo is not None
    assert fetched.occurred_at.utcoffset() == aware_dt.utcoffset()
    assert fetched.occurred_at == aware_dt
    client.__class__.close_all()


def test_datetime_naive_roundtrip(tmp_path: Path) -> None:
    url = f"sqlite:///{(tmp_path / 'naive.db').as_posix()}"
    globals()["__datasource__"] = {"provider": "sqlite", "url": url}

    _prepare(url, Event)
    module = generate_client([Event])
    ns: dict[str, Any] = {}
    exec(module.code, ns)
    client = ns["Client"]()
    table = client.event
    naive_dt = datetime(2024, 1, 1, 8, 0, 0)

    inserted = table.insert({"id": 1, "occurred_at": naive_dt})
    fetched = table.find_first(where={"id": 1})

    assert inserted.occurred_at.tzinfo is None
    assert fetched is not None and fetched.occurred_at.tzinfo is None
    assert fetched.occurred_at == naive_dt
    client.__class__.close_all()
