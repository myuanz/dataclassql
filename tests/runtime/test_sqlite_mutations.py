from __future__ import annotations

from pathlib import Path

from dclassql import record_sql

from .conftest import prepare_database, build_client


def _insert_three(namespace, user_table):
    InsertModel = namespace["RuntimeUserInsert"]
    user_table.insert_many(
        [
            InsertModel(id=None, name="Foo", email=None),
            {"id": None, "name": "Bar", "email": "bar@example.com"},
            {"id": None, "name": "Baz", "email": "baz@example.com"},
        ]
    )


def test_delete_returns_removed_row(tmp_path: Path) -> None:
    db_path = tmp_path / "delete.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    first = user_table.insert(InsertModel(id=None, name="Alice", email=None))
    user_table.insert({"id": None, "name": "Bob", "email": "bob@example.com"})

    deleted = user_table.delete(where={"id": first.id})
    assert deleted is not None
    assert deleted.id == first.id
    assert user_table.find_first(where={"id": first.id}) is None

    missing = user_table.delete(where={"id": 999})
    assert missing is None
    client.__class__.close_all()


def test_delete_many_supports_count_and_records(tmp_path: Path) -> None:
    db_path = tmp_path / "delete_many.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user

    _insert_three(namespace, user_table)

    deleted_count = user_table.delete_many(where={"email": None})
    assert deleted_count == 1

    remaining_names = [row.name for row in user_table.find_many(order_by={"id": "asc"})]
    assert remaining_names == ["Bar", "Baz"]

    removed_records = user_table.delete_many(where={"name": {"IN": ["Bar", "Baz"]}}, return_records=True)
    removed_names = sorted(row.name for row in removed_records)
    assert removed_names == ["Bar", "Baz"]

    assert user_table.find_first(order_by={"id": "asc"}) is None
    client.__class__.close_all()


def test_update_returns_updated_row(tmp_path: Path) -> None:
    db_path = tmp_path / "update.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    inserted = user_table.insert(InsertModel(id=None, name="Old", email=None))

    updated = user_table.update(data={"name": "New", "email": "new@example.com"}, where={"id": inserted.id})
    assert updated.id == inserted.id
    assert updated.name == "New"
    assert updated.email == "new@example.com"

    fetched = user_table.find_first(where={"id": inserted.id})
    assert fetched.name == "New"
    client.__class__.close_all()


def test_update_many_supports_count_and_records(tmp_path: Path) -> None:
    db_path = tmp_path / "update_many.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user

    _insert_three(namespace, user_table)

    updated_count = user_table.update_many(data={"email": "shared@example.com"}, where={"email": None})
    assert updated_count == 1

    updated_records = user_table.update_many(
        data={"name": "Renamed"},
        where={"email": {"EQ": "shared@example.com"}},
        return_records=True,
    )
    assert [row.name for row in updated_records] == ["Renamed"]

    remaining = user_table.find_many(order_by={"id": "asc"})
    assert [row.name for row in remaining] == ["Renamed", "Bar", "Baz"]
    client.__class__.close_all()


def test_upsert_inserts_when_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "upsert_insert.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    created = user_table.upsert(
        where={"id": 1},
        update={"name": "Updated", "email": "u@example.com"},
        insert=InsertModel(id=1, name="Created", email=None),
    )
    assert created.id == 1
    assert created.name == "Created"

    fetched = user_table.find_first(where={"id": 1})
    assert fetched.name == "Created"
    client.__class__.close_all()


def test_upsert_updates_on_conflict(tmp_path: Path) -> None:
    db_path = tmp_path / "upsert_update.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="Origin", email="o@example.com"))

    updated = user_table.upsert(
        where={"id": 1},
        update={"name": "Upserted"},
        insert={"id": 1, "name": "ShouldNotUse", "email": None},
    )
    assert updated.id == 1
    assert updated.name == "Upserted"

    fetched = user_table.find_first(where={"id": 1})
    assert fetched.name == "Upserted"
    client.__class__.close_all()


def test_upsert_records_sql(tmp_path: Path) -> None:
    db_path = tmp_path / "upsert_sql.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table._backend._echo_sql = False

    with record_sql(echo=False) as sqls:
        user_table.upsert(
            where={"id": 1},
            update={"name": "Upserted"},
            insert=InsertModel(id=1, name="Inserted", email=None),
        )
    assert sqls == [
        (
            'INSERT INTO "RuntimeUser" ("id","name","email") VALUES (?,?,?) ON CONFLICT ("id") DO UPDATE SET "name" = ? RETURNING "id", "name", "email";',
            (1, "Inserted", None, "Upserted"),
        )
    ]
    client.__class__.close_all()


def test_record_sql_insert_update_delete(tmp_path: Path) -> None:
    db_path = tmp_path / "sql_log.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    with record_sql() as sqls:
        inserted = user_table.insert(InsertModel(id=None, name="Alice", email=None))
    assert sqls == [
        ('INSERT INTO "RuntimeUser" ("id","name","email") VALUES (?,?,?) RETURNING "id", "name", "email";', (None, "Alice", None))
    ]

    with record_sql() as sqls:
        user_table.update(data={"name": "Bob"}, where={"id": inserted.id})
    assert sqls == [
        ('UPDATE "RuntimeUser" SET "name"=? WHERE "id"=? RETURNING "id", "name", "email";', ("Bob", inserted.id))
    ]

    with record_sql() as sqls:
        user_table.delete_many(where={"id": {"IN": [inserted.id]}})
    assert sqls == [
        ('DELETE FROM "RuntimeUser" WHERE "id" IN (?) RETURNING "id", "name", "email";', (inserted.id,))
    ]

    client.__class__.close_all()
