from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///test.db",
}

from typed_db.model_inspector import inspect_models
from typed_db.push import db_push
from typed_db.push.sqlite import _build_sqlite_schema, push_sqlite


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
        '"email" TEXT,"created_at" TEXT NOT NULL,UNIQUE ("email"));'
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
        '"email" TEXT,"created_at" TEXT NOT NULL,UNIQUE ("email"))'
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
