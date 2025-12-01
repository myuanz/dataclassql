from __future__ import annotations

from pathlib import Path

from dclassql import record_sql

from .conftest import build_client, prepare_database


def test_where_filters_support_scalar_operations(tmp_path: Path):
    db_path = tmp_path / "filters.db"
    prepare_database(db_path)
    namespace, client = build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert_many(
        [
            InsertModel(id=None, name="Alice", email="alice@example.com"),
            InsertModel(id=None, name="Bob", email=None),
            InsertModel(id=None, name="Charlie", email="charlie@example.com"),
        ]
    )

    with record_sql() as sqls:
        contains_results = user_table.find_many(where={"name": {"CONTAINS": "li"}}, order_by={"name": "asc"})
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "name" LIKE ? ORDER BY "name" ASC;', ("%li%",))]
    assert [row.name for row in contains_results] == ["Alice", "Charlie"]

    with record_sql() as sqls:
        and_results = user_table.find_many(
            where={
                "AND": [
                    {"name": {"STARTS_WITH": "A"}},
                    {"email": {"EQ": "alice@example.com"}},
                ]
            }
        )
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "name" LIKE ? AND "email"=?;', ("A%", "alice@example.com"))]
    assert [row.name for row in and_results] == ["Alice"]

    with record_sql() as sqls:
        or_results = user_table.find_many(where={"OR": [{"name": {"EQ": "Alice"}}, {"name": {"EQ": "Bob"}}]}, order_by={"name": "asc"})
    assert sqls == [
        ('SELECT "id","name","email" FROM "RuntimeUser" WHERE "name"=? OR "name"=? ORDER BY "name" ASC;', ("Alice", "Bob"))
    ]
    assert [row.name for row in or_results] == ["Alice", "Bob"]

    with record_sql() as sqls:
        in_results = user_table.find_many(where={"id": {"IN": [1, 3]}}, order_by={"id": "asc"})
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "id" IN (?,?) ORDER BY "id" ASC;', (1, 3))]
    assert [row.name for row in in_results] == ["Alice", "Charlie"]

    with record_sql() as sqls:
        not_results = user_table.find_many(where={"name": {"NOT": "Alice"}}, order_by={"name": "asc"})
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE NOT "name"=? ORDER BY "name" ASC;', ("Alice",))]
    assert [row.name for row in not_results] == ["Bob", "Charlie"]

    with record_sql() as sqls:
        null_results = user_table.find_many(where={"email": None})
    assert [row.name for row in null_results] == ["Bob"]
    assert sqls == [('SELECT "id","name","email" FROM "RuntimeUser" WHERE "email" IS NULL;', ())]

    client.__class__.close_all()
