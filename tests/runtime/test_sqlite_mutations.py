from __future__ import annotations

from pathlib import Path

from dclassql import record_sql

from .conftest import build_client, prepare_database


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

    with record_sql() as sqls:
        deleted = user_table.delete(where={"id": first.id})
    assert sqls == [
        (
            'DELETE FROM "RuntimeUser" WHERE "id"=? RETURNING "id", "name", "email";',
            (first.id,),
        )
    ]
    assert deleted is not None
    assert deleted.id == first.id
    assert user_table.find_first(where={"id": first.id}) is None

    with record_sql() as sqls:
        missing = user_table.delete(where={"id": 999})
    assert sqls == [
        ('DELETE FROM "RuntimeUser" WHERE "id"=? RETURNING "id", "name", "email";', (999,))
    ]
    assert missing is None
    client.__class__.close_all()


def test_delete_many_supports_count_and_records(tmp_path: Path) -> None:
    db_path = tmp_path / "delete_many.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user

    _insert_three(namespace, user_table)

    with record_sql() as sqls:
        deleted_count = user_table.delete_many(where={"email": None})
    assert sqls == [('DELETE FROM "RuntimeUser" WHERE "email" IS NULL RETURNING "id", "name", "email";', ())]
    assert deleted_count == 1

    remaining_names = [row.name for row in user_table.find_many(order_by={"id": "asc"})]
    assert remaining_names == ["Bar", "Baz"]

    with record_sql() as sqls:
        removed_records = user_table.delete_many(where={"name": {"IN": ["Bar", "Baz"]}}, return_records=True)
    assert sqls == [
        ('DELETE FROM "RuntimeUser" WHERE "name" IN (?,?) RETURNING "id", "name", "email";', ("Bar", "Baz"))
    ]
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

    with record_sql() as sqls:
        updated = user_table.update(data={"name": "New", "email": "new@example.com"}, where={"id": inserted.id})
    assert sqls == [
        (
            'UPDATE "RuntimeUser" SET "name"=?,"email"=? WHERE "id"=? RETURNING "id", "name", "email";',
            ("New", "new@example.com", inserted.id),
        )
    ]
    assert updated.id == inserted.id
    assert updated.name == "New"
    assert updated.email == "new@example.com"

    with record_sql() as sqls:
        fetched = user_table.find_first(where={"id": inserted.id})
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "id"=? LIMIT 1;', (inserted.id,))]
    assert fetched.name == "New"
    client.__class__.close_all()


def test_update_many_supports_count_and_records(tmp_path: Path) -> None:
    db_path = tmp_path / "update_many.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user

    _insert_three(namespace, user_table)

    with record_sql() as sqls:
        updated_count = user_table.update_many(data={"email": "shared@example.com"}, where={"email": None})
    assert sqls == [
        ('UPDATE "RuntimeUser" SET "email"=? WHERE "email" IS NULL RETURNING "id", "name", "email";', ("shared@example.com",))
    ]
    assert updated_count == 1

    with record_sql() as sqls:
        updated_records = user_table.update_many(
            data={"name": "Renamed"},
            where={"email": {"EQ": "shared@example.com"}},
            return_records=True,
        )
    assert sqls == [
        ('UPDATE "RuntimeUser" SET "name"=? WHERE "email"=? RETURNING "id", "name", "email";', ("Renamed", "shared@example.com"))
    ]
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

    with record_sql() as sqls:
        created = user_table.upsert(
            where={"id": 1},
            update={"name": "Updated", "email": "u@example.com"},
            insert=InsertModel(id=1, name="Created", email=None),
        )
    assert sqls == [
        (
            'INSERT INTO "RuntimeUser" ("id","name","email") VALUES (?,?,?) ON CONFLICT ("id") DO UPDATE SET "name" = ?, "email" = ? RETURNING "id", "name", "email";',
            (1, "Created", None, "Updated", "u@example.com"),
        )
    ]
    assert created.id == 1
    assert created.name == "Created"

    with record_sql() as sqls:
        fetched = user_table.find_first(where={"id": 1})
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "id"=? LIMIT 1;', (1,))]
    assert fetched.name == "Created"
    client.__class__.close_all()


def test_upsert_updates_on_conflict(tmp_path: Path) -> None:
    db_path = tmp_path / "upsert_update.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="Origin", email="o@example.com"))

    with record_sql() as sqls:
        updated = user_table.upsert(
            where={"id": 1},
            update={"name": "Upserted"},
            insert={"id": 1, "name": "ShouldNotUse", "email": None},
        )
    assert sqls == [
        (
            'INSERT INTO "RuntimeUser" ("id","name","email") VALUES (?,?,?) ON CONFLICT ("id") DO UPDATE SET "name" = ? RETURNING "id", "name", "email";',
            (1, "ShouldNotUse", None, "Upserted"),
        )
    ]
    assert updated.id == 1
    assert updated.name == "Upserted"

    with record_sql() as sqls:
        fetched = user_table.find_first(where={"id": 1})
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "id"=? LIMIT 1;', (1,))]
    assert fetched.name == "Upserted"
    client.__class__.close_all()
