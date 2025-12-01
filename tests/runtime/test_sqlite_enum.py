from __future__ import annotations

from pathlib import Path

from dclassql import record_sql
from dclassql.push import db_push
from dclassql.runtime.datasource import open_sqlite_connection

from .conftest import (
    IntEnumTest,
    RuntimeEnumUser,
    RuntimeState,
    StrEnumTest,
    build_enum_client,
    prepare_database,
)


def test_enum_field_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "enum_runtime.db"
    prepare_database(db_path)

    # push enum table
    conn = open_sqlite_connection(f"sqlite:///{db_path.as_posix()}")
    try:
        db_push([RuntimeEnumUser], {"sqlite": conn})
        conn.execute('DELETE FROM "RuntimeEnumUser"')
        conn.commit()
    finally:
        conn.close()

    namespace, client = build_enum_client()
    table = client.runtime_enum_user
    InsertCls = namespace["RuntimeEnumUserInsert"]

    with record_sql() as sqls:
        inserted = table.insert(InsertCls(id=None, state=RuntimeState.ACTIVE, s1=StrEnumTest.SECOND, s2=IntEnumTest.TWO))
    assert sqls == [
        (
            'INSERT INTO "RuntimeEnumUser" ("id","state","s1","s2") VALUES (?,?,?,?) RETURNING "id", "state", "s1", "s2";',
            (None, RuntimeState.ACTIVE.value, StrEnumTest.SECOND.value, IntEnumTest.TWO.value),
        )
    ]
    assert inserted.state is RuntimeState.ACTIVE

    with record_sql() as sqls:
        fetched = table.find_first(order_by={"id": "asc"})
    assert sqls == [
        ('SELECT "id","state","s1","s2" FROM "RuntimeEnumUser" ORDER BY "id" ASC LIMIT 1;', ())
    ]
    assert fetched is not None
    assert fetched.state is RuntimeState.ACTIVE
    assert isinstance(fetched.state, RuntimeState)
    assert fetched.s1 is StrEnumTest.SECOND
    assert isinstance(fetched.s1, StrEnumTest)
    assert fetched.s2 is IntEnumTest.TWO
    assert isinstance(fetched.s2, IntEnumTest)

    client.__class__.close_all()

    conn = open_sqlite_connection(f"sqlite:///{db_path.as_posix()}")
    try:
        stored = conn.execute('SELECT state FROM "RuntimeEnumUser" ORDER BY id').fetchone()[0]
    finally:
        conn.close()
    assert stored == RuntimeState.ACTIVE.value
