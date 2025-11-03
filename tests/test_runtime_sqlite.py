from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from typed_db.codegen import generate_client
from typed_db.push import db_push


__datasource__ = {"provider": "sqlite", "url": None}


@dataclass
class RuntimeUser:
    id: int | None
    name: str
    email: str | None


def _prepare_database(db_path: Path) -> None:
    global __datasource__
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:////{db_path.as_posix()}"}
    conn = sqlite3.connect(db_path)
    try:
        db_push([RuntimeUser], {"sqlite": conn})
    finally:
        conn.close()


def _build_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    generated_client = namespace["GeneratedClient"]
    expected_url = __datasource__["url"]
    assert generated_client.datasources[[*generated_client.datasources.keys()][0]].url == expected_url
    client = generated_client()
    return namespace, client


def test_insert_and_find_roundtrip(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    stored = user_table.insert(InsertModel(id=None, name="Alice", email="alice@example.com"))
    assert stored.id is not None
    assert stored.name == "Alice"
    assert stored.email == "alice@example.com"

    stored_dict = user_table.insert({"id": None, "name": "Bob", "email": None})
    assert stored_dict.name == "Bob"
    assert stored_dict.email is None

    fetched = user_table.find_many(where={"name": "Alice"})
    assert [user.name for user in fetched] == ["Alice"]

    ordered = user_table.find_many(order_by=[("name", "desc")])
    assert [user.name for user in ordered] == ["Bob", "Alice"]

    first = user_table.find_first(order_by=[("name", "asc")])
    assert first.name == "Alice"
    client.__class__.close_all()


def test_insert_many_utilises_backend(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    rows = [
        InsertModel(id=None, name="Carol", email=None),
        {"id": None, "name": "Dave", "email": "dave@example.com"},
    ]
    inserted = user_table.insert_many(rows, batch_size=1)
    assert [user.name for user in inserted] == ["Carol", "Dave"]

    all_rows = user_table.find_many(order_by=[("name", "asc")])
    assert [user.name for user in all_rows] == ["Carol", "Dave"]
    client.__class__.close_all()


def test_insert_many_generates_sequential_ids(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    inserted = user_table.insert_many(
        [
            InsertModel(id=None, name="Foo", email=None),
            InsertModel(id=None, name="Bar", email=None),
            InsertModel(id=None, name="Baz", email=None),
        ],
        batch_size=2,
    )

    ids = [user.id for user in inserted]
    assert ids == [1, 2, 3]
    client.__class__.close_all()


def test_backend_thread_local(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="Eve", email=None))

    def worker() -> int | None:
        other_client = namespace["GeneratedClient"]()
        try:
            record = other_client.runtime_user.find_first(order_by=[("name", "asc")])
            return record.id if record else None
        finally:
            other_client.__class__.close_all()

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(worker)
        thread_result = future.result()

    main_record = user_table.find_first(order_by=[("name", "asc")])
    assert main_record is not None
    assert thread_result is not None
    client.__class__.close_all()


def test_find_many_rejects_unknown_columns(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    _, client = _build_client()
    user_table = client.runtime_user

    with pytest.raises(KeyError):
        user_table.find_many(where={"unknown": "value"})

    with pytest.raises(ValueError):
        user_table.find_many(order_by=[("name", "sideways")])
    client.__class__.close_all()


def test_include_not_supported(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    _, client = _build_client()
    user_table = client.runtime_user

    with pytest.raises(NotImplementedError):
        user_table.find_many(include={"anything": True})
    client.__class__.close_all()
