from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dclassql import record_sql

from .conftest import prepare_database, build_client


def test_find_many_rejects_unknown_columns(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()
    user_table = client.runtime_user

    with pytest.raises(KeyError):
        user_table.find_many(where={"unknown": "value"})

    with pytest.raises(ValueError):
        user_table.find_many(order_by={"name": "sideways"})

    with pytest.raises(sqlite3.OperationalError):
        user_table.find_many(order_by={"bad": "asc"}, distinct="email")
    client.__class__.close_all()


def test_unknown_write_columns_reach_sqlite(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()
    user_table = client.runtime_user

    with pytest.raises(sqlite3.OperationalError):
        user_table.insert({"name": "Insert", "email": None, "typo": 1})

    with pytest.raises(sqlite3.OperationalError):
        user_table.insert_many([
            {"name": "InsertMany", "email": None, "typo": 1},
        ])

    inserted = user_table.insert({"name": "Stable", "email": None})

    with pytest.raises(sqlite3.OperationalError):
        user_table.update(
            data={"name": "Update", "typo": 1},
            where={"id": inserted.id},
        )

    with pytest.raises(sqlite3.OperationalError):
        user_table.update_many(
            data={"email": "update-many@example.com", "typo": 1},
            where={"id": inserted.id},
        )

    with pytest.raises(sqlite3.OperationalError):
        user_table.upsert(
            where={"id": inserted.id},
            update={"name": "Upsert", "typo": 1},
            insert={"id": inserted.id, "name": "Stable", "email": None},
        )

    fetched = user_table.find_first(where={"id": inserted.id})
    assert fetched is not None
    assert fetched.name == "Stable"
    assert fetched.email is None
    client.__class__.close_all()


def test_order_by_unknown_column_reaches_sqlite_and_quotes_identifier(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()
    user_table = client.runtime_user
    user_table.insert({"name": "Safe", "email": None})

    with record_sql() as sqls:
        with pytest.raises(sqlite3.OperationalError):
            user_table.find_many(order_by={"bad": "asc"})
    assert sqls == [
        (
            'SELECT "t"."id","t"."name","t"."email" FROM "RuntimeUser" "t" '
            'ORDER BY "t"."bad" ASC;',
            (),
        )
    ]

    malicious_column = 'name"; DROP TABLE "RuntimeUser"; --'
    with record_sql() as sqls:
        with pytest.raises(sqlite3.OperationalError):
            user_table.find_many(order_by={malicious_column: "asc"})
    assert sqls == [
        (
            'SELECT "t"."id","t"."name","t"."email" FROM "RuntimeUser" "t" '
            'ORDER BY "t"."name""; DROP TABLE ""RuntimeUser""; --" ASC;',
            (),
        )
    ]

    assert [row.name for row in user_table.find_many()] == ["Safe"]
    client.__class__.close_all()


def test_include_not_supported(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()
    user_table = client.runtime_user

    result = user_table.find_many(include={"anything": True})
    assert result == []
    client.__class__.close_all()
