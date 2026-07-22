from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import pytest

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///test.db",
}

from dclassql.codegen import generate_client
from dclassql.push import db_push
from dclassql.push.sqlite import _build_sqlite_schema, push_sqlite
from dclassql.runtime.backends.protocols import SchemaTableProtocol


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


type UserKind = Literal["retail", "vip"]


@dataclass
class AliasUser:
    id: int
    kind: UserKind


@dataclass
class Event:
    name: str
    created_at: datetime


def generated_tables(*models: type[Any]) -> list[SchemaTableProtocol]:
    generated = generate_client(list(models))
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    return [namespace[f"{model.__name__}Table"] for model in models]


def test_db_push_creates_table_and_indexes():
    table = generated_tables(User)[0]
    create_sql, index_entries = _build_sqlite_schema(table)

    assert create_sql == (
        'CREATE TABLE IF NOT EXISTS "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"email" TEXT,"created_at" datetime NOT NULL);'
    )
    assert index_entries == [
        ('idx_User_name', 'CREATE INDEX IF NOT EXISTS "idx_User_name" ON "User" ("name");'),
        ('idx_User_created_at', 'CREATE INDEX IF NOT EXISTS "idx_User_created_at" ON "User" ("created_at");'),
        ('uq_User_email', 'CREATE UNIQUE INDEX IF NOT EXISTS "uq_User_email" ON "User" ("email");'),
    ]

    conn = sqlite3.connect(":memory:")
    push_sqlite(conn, [table])

    rows = conn.execute(
        "SELECT type,name,sql FROM sqlite_master WHERE tbl_name='User' ORDER BY type,name"
    ).fetchall()
    table_sql = next(sql for (typ, name, sql) in rows if typ == 'table')
    assert table_sql == (
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"email" TEXT,"created_at" datetime NOT NULL)'
    )
    index_sqls = {name: sql for (typ, name, sql) in rows if typ == 'index' and sql}
    assert index_sqls['idx_User_name'] == 'CREATE INDEX "idx_User_name" ON "User" ("name")'
    assert index_sqls['idx_User_created_at'] == 'CREATE INDEX "idx_User_created_at" ON "User" ("created_at")'
    assert index_sqls['uq_User_email'] == 'CREATE UNIQUE INDEX "uq_User_email" ON "User" ("email")'
    index_rows = conn.execute('PRAGMA index_list("User")').fetchall()
    assert {(row[1], row[2], row[3]) for row in index_rows} == {
        ("idx_User_name", 0, "c"),
        ("idx_User_created_at", 0, "c"),
        ("uq_User_email", 1, "c"),
    }

    conn.execute(
        'INSERT INTO "User" ("name","email","created_at") VALUES (?,?,?)',
        ("Alice", "alice@example.com", "2026-01-01T00:00:00"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            'INSERT INTO "User" ("name","email","created_at") VALUES (?,?,?)',
            ("Alice 2", "alice@example.com", "2026-01-02T00:00:00"),
        )

    conn2 = sqlite3.connect(":memory:")
    db_push([table], conn2, provider="sqlite")
    assert (
        conn2.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE tbl_name='User' AND type='table'"
        ).fetchone()[0]
        == 1
    )


def test_db_push_infers_type_alias_value():
    table = generated_tables(AliasUser)[0]
    create_sql, _ = _build_sqlite_schema(table)

    assert '"kind" TEXT NOT NULL' in create_sql


def test_db_push_adds_implicit_id_primary_key_for_model_without_id():
    table = generated_tables(Event)[0]
    create_sql, _ = _build_sqlite_schema(table)

    assert create_sql == (
        'CREATE TABLE IF NOT EXISTS "Event" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"created_at" datetime NOT NULL);'
    )

    conn = sqlite3.connect(":memory:")
    push_sqlite(conn, [table])
    conn.execute(
        'INSERT INTO "Event" ("name","created_at") VALUES (?, ?)',
        ("start", "2026-01-01T00:00:00"),
    )

    rows = conn.execute('SELECT id,name,created_at FROM "Event"').fetchall()
    assert rows == [(1, "start", "2026-01-01T00:00:00")]


def test_db_push_sync_indexes_aligns_with_model():
    table = generated_tables(User)[0]
    conn = sqlite3.connect(":memory:")
    push_sqlite(conn, [table])

    conn.execute('CREATE INDEX "idx_User_extra" ON "User" ("created_at", "name")')
    conn.execute('DROP INDEX "idx_User_name"')

    db_push([table], conn, provider="sqlite", sync_indexes=True)

    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='User'"
    ).fetchall()

    index_names = {name for (name,) in rows if not name.startswith("sqlite_")}
    assert index_names == {
        "idx_User_name",
        "idx_User_created_at",
        "uq_User_email",
    }


def test_db_push_without_sync_indexes_leaves_extra_indexes():
    table = generated_tables(User)[0]
    conn = sqlite3.connect(":memory:")
    push_sqlite(conn, [table])
    conn.execute('CREATE INDEX "idx_User_extra" ON "User" ("created_at", "name")')

    db_push([table], conn, provider="sqlite")

    index_names = {
        name
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='User'"
        ).fetchall()
    }
    assert "idx_User_extra" in index_names


def test_db_push_rebuild_requires_confirmation():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL)'
    )

    with pytest.raises(RuntimeError) as exc:
        db_push(generated_tables(User), conn, provider="sqlite")

    assert "新增列" in str(exc.value)


def test_db_push_rebuilds_table_and_preserves_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"created_at" datetime NOT NULL)'
    )
    conn.execute('CREATE INDEX "idx_User_extra" ON "User" ("created_at", "name")')
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

    db_push(generated_tables(User), conn, provider="sqlite", confirm_rebuild=approve_rebuild)

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


def test_db_push_rebuild_rolls_back_when_unique_index_cannot_be_created():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "User" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL,'
        '"email" TEXT,"created_at" TEXT NOT NULL)'
    )
    conn.executemany(
        'INSERT INTO "User" ("name","email","created_at") VALUES (?,?,?)',
        [
            ("Alice", "same@example.com", "2024-01-01T00:00:00"),
            ("Bob", "same@example.com", "2024-01-02T00:00:00"),
        ],
    )

    with pytest.raises(sqlite3.IntegrityError):
        db_push(
            generated_tables(User),
            conn,
            provider="sqlite",
            confirm_rebuild=lambda *_args: True,
        )

    columns = conn.execute('PRAGMA table_info("User")').fetchall()
    assert next(row[2] for row in columns if row[1] == "created_at") == "TEXT"
    assert conn.execute('SELECT name FROM "User" ORDER BY id').fetchall() == [
        ("Alice",),
        ("Bob",),
    ]


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

    db_push(generated_tables(User), conn, provider="sqlite", confirm_rebuild=approve)

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

    db_push(generated_tables(User), conn, provider="sqlite", confirm_rebuild=approve)

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
        db_push(generated_tables(User), conn, provider="sqlite", confirm_rebuild=reject)

    assert "模型 User" in str(exc.value)
