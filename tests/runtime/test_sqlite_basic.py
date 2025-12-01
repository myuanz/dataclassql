from __future__ import annotations

from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor

from .conftest import prepare_database, build_client, RuntimeUser


def test_insert_and_find_roundtrip(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    namespace, client = build_client()
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

    ordered = user_table.find_many(order_by={"name": "desc"})
    assert [user.name for user in ordered] == ["Bob", "Alice"]

    first = user_table.find_first(order_by={"name": "asc"})
    assert first.name == "Alice"
    client.__class__.close_all()


def test_find_returns_distinct_instances(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="Alice", email=None))

    first = user_table.find_first(order_by={"id": "asc"})
    second = user_table.find_first(order_by={"id": "asc"})

    assert first is not None and second is not None
    assert first.id == second.id
    assert first is not second

    client.__class__.close_all()


def test_table_str_uses_backend_quotes(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()

    table = client.runtime_user
    assert table.table_name == "RuntimeUser"
    assert str(table) == '"RuntimeUser"'

    client.__class__.close_all()


def test_backend_raw_queries(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()
    backend = client.runtime_user._backend

    inserted = backend.execute_raw(
        'INSERT INTO "RuntimeUser" (id, name, email) VALUES (?, ?, ?)',
        (1, "Alice", None),
    )
    assert inserted == 1

    rows = backend.query_raw('SELECT id, name, email FROM "RuntimeUser" WHERE id = ?', (1,))
    assert isinstance(rows, list)
    assert rows[0]["name"] == "Alice"

    updated = backend.execute_raw(
        'UPDATE "RuntimeUser" SET name = ? WHERE id = ?',
        ("Alicia", 1),
    )
    assert updated == 1

    total_rows = backend.query_raw('SELECT COUNT(1) as c FROM "RuntimeUser"')
    assert total_rows[0]["c"] == 1

    client.__class__.close_all()


def test_insert_many_utilises_backend(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    rows = [
        InsertModel(id=None, name="Carol", email=None),
        {"id": None, "name": "Dave", "email": "dave@example.com"},
    ]
    inserted = user_table.insert_many(rows, batch_size=1)
    assert [user.name for user in inserted] == ["Carol", "Dave"]

    all_rows = user_table.find_many(order_by={"name": "asc"})
    assert [user.name for user in all_rows] == ["Carol", "Dave"]
    client.__class__.close_all()


def test_insert_many_generates_sequential_ids(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    namespace, client = build_client()
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
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user

    user_table.insert({'name': "Eve", 'email': None})

    def worker() -> int | None:
        other_client = namespace["Client"]()
        try:
            record = other_client.runtime_user.find_first(order_by={"name": "asc"})
            return record.id if record else None
        finally:
            other_client.__class__.close_all()

    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(worker)
        thread_result = future.result()

    main_record = user_table.find_first(order_by={"name": "asc"})
    assert main_record is not None
    assert thread_result is not None
    client.__class__.close_all()


def test_find_many_supports_distinct(tmp_path: Path) -> None:
    db_path = tmp_path / "distinct.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="n1", email="shared@example.com"))
    user_table.insert(InsertModel(id=None, name="n2", email="shared@example.com"))
    user_table.insert(InsertModel(id=None, name="n3", email="unique@example.com"))

    distinct_users = user_table.find_many(order_by={"id": "asc"}, distinct="email")
    assert [user.email for user in distinct_users] == ["shared@example.com", "unique@example.com"]

    distinct_second = user_table.find_many(order_by={"id": "asc"}, distinct="email", skip=1)
    assert [user.email for user in distinct_second] == ["unique@example.com"]

    multi_column = user_table.find_many(distinct=["email", "name"], order_by={"id": "asc"})
    assert [user.name for user in multi_column] == ["n1", "n2", "n3"]

    client.__class__.close_all()
