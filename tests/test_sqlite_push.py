from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pytest

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///test.db",
}

from dclassql.model_inspector import inspect_models
from dclassql.push import db_push
from dclassql.push.sqlite import _build_sqlite_schema, push_sqlite


@dataclass
class User:
    id: int
    name: str
    email: str | None
    created_at: datetime

    def index(self):
        yield self.name
        yield self.created_at

    def unique_index(self):
        yield self.email


def test_db_push_creates_table_and_indexes():
    info = inspect_models([User])["User"]
    create_sql, index_entries = _build_sqlite_schema(info)

    assert create_sql == (
        'CREATE TABLE IF NOT EXISTS "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"email" TEXT,"created_at" datetime NOT NULL,UNIQUE ("email"));'
    )
    assert index_entries == [
        ('idx_User_name', 'CREATE INDEX IF NOT EXISTS "idx_User_name" ON "User" ("name");'),
        ('idx_User_created_at', 'CREATE INDEX IF NOT EXISTS "idx_User_created_at" ON "User" ("created_at");'),
        ('uq_User_email', 'CREATE UNIQUE INDEX IF NOT EXISTS "uq_User_email" ON "User" ("email");'),
    ]

    conn = sqlite3.connect(":memory:")
    push_sqlite(conn, [info])

    rows = conn.execute(
        "SELECT type,name,sql FROM sqlite_master WHERE tbl_name='User' ORDER BY type,name"
    ).fetchall()
    table_sql = next(sql for (typ, name, sql) in rows if typ == 'table')
    assert table_sql == (
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"email" TEXT,"created_at" datetime NOT NULL,UNIQUE ("email"))'
    )
    index_sqls = {name: sql for (typ, name, sql) in rows if typ == 'index' and sql}
    assert index_sqls['idx_User_name'] == 'CREATE INDEX "idx_User_name" ON "User" ("name")'
    assert index_sqls['idx_User_created_at'] == 'CREATE INDEX "idx_User_created_at" ON "User" ("created_at")'
    assert index_sqls['uq_User_email'] == 'CREATE UNIQUE INDEX "uq_User_email" ON "User" ("email")'

    conn2 = sqlite3.connect(":memory:")
    db_push([User], {"sqlite": conn2})
    assert (
        conn2.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE tbl_name='User' AND type='table'"
        ).fetchone()[0]
        == 1
    )


def test_db_push_sync_indexes_aligns_with_model():
    info = inspect_models([User])["User"]
    conn = sqlite3.connect(":memory:")
    push_sqlite(conn, [info])

    conn.execute('CREATE INDEX "idx_User_extra" ON "User" ("created_at", "name")')
    conn.execute('DROP INDEX "idx_User_name"')

    db_push([User], {"sqlite": conn}, sync_indexes=True)

    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='User'"
    ).fetchall()

    index_names = {name for (name,) in rows if not name.startswith("sqlite_")}
    assert index_names == {
        "idx_User_name",
        "idx_User_created_at",
        "uq_User_email",
    }


def test_db_push_rebuild_requires_confirmation():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL)'
    )

    with pytest.raises(RuntimeError) as exc:
        db_push([User], {"sqlite": conn})

    assert "新增列" in str(exc.value)


def test_db_push_rebuilds_table_and_preserves_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"created_at" datetime NOT NULL)'
    )
    conn.execute(
        'INSERT INTO "User" ("name","created_at") VALUES (?, ?)',
        ("Alice", "2024-01-01T12:00:00"),
    )

    captured_diff: dict[str, tuple[str, ...]] = {}

    def approve_rebuild(info, plan, existing, diff):
        captured_diff["added"] = tuple(column.name for column in diff.added)
        captured_diff["removed"] = tuple(column.name for column in diff.removed)
        captured_diff["changed"] = tuple(change.name for change in diff.changed)
        return True

    db_push([User], {"sqlite": conn}, confirm_rebuild=approve_rebuild)

    columns = conn.execute('PRAGMA table_info("User")').fetchall()
    column_names = [name for (_, name, *_rest) in columns]
    assert column_names == ["id", "name", "email", "created_at"]

    assert captured_diff["added"] == ("email",)
    assert captured_diff["removed"] == ()
    assert captured_diff["changed"] == ()

    rows = conn.execute('SELECT id,name,email,created_at FROM "User"').fetchall()
    assert rows == [(1, "Alice", None, "2024-01-01T12:00:00")]

    index_entries = conn.execute('PRAGMA index_list("User")').fetchall()
    index_names = {entry[1] for entry in index_entries if not entry[1].startswith("sqlite_")}
    assert index_names == {
        "idx_User_name",
        "idx_User_created_at",
        "uq_User_email",
    }


def test_db_push_rebuild_drops_extra_columns():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"email" TEXT,"created_at" TEXT NOT NULL,"legacy" TEXT)'
    )
    conn.execute(
        'INSERT INTO "User" ("name","email","created_at","legacy") VALUES (?,?,?,?)',
        ("Bob", "bob@example.com", "2024-02-02T00:00:00", "old"),
    )

    def approve(_, __, ___, diff):
        assert tuple(column.name for column in diff.removed) == ("legacy",)
        return True

    db_push([User], {"sqlite": conn}, confirm_rebuild=approve)

    columns = conn.execute('PRAGMA table_info("User")').fetchall()
    column_names = [name for (_, name, *_rest) in columns]
    assert column_names == ["id", "name", "email", "created_at"]

    restored = conn.execute('SELECT name,email,created_at FROM "User"').fetchall()
    assert restored == [("Bob", "bob@example.com", "2024-02-02T00:00:00")]


def test_db_push_rebuild_detects_column_type_change():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" INTEGER NOT NULL,'
        '"email" TEXT,"created_at" datetime NOT NULL)'
    )

    def approve(_, __, ___, diff):
        changed = {change.name: change.reasons for change in diff.changed}
        assert changed == {"name": ("type INTEGER -> TEXT",)}
        return True

    db_push([User], {"sqlite": conn}, confirm_rebuild=approve)

    column_info = conn.execute('PRAGMA table_info("User")').fetchall()
    types = {name: typ for (_, name, typ, *_rest) in column_info}
    assert types["name"] == "TEXT"


def test_db_push_rebuild_callback_can_abort():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL)'
    )

    def reject(*_args):
        return False

    with pytest.raises(RuntimeError) as exc:
        db_push([User], {"sqlite": conn}, confirm_rebuild=reject)

    assert "模型 User" in str(exc.value)
