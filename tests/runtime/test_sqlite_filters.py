from __future__ import annotations

from pathlib import Path

from .conftest import prepare_database, build_client


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

    contains_results = user_table.find_many(where={"name": {"CONTAINS": "li"}}, order_by={"name": "asc"})
    assert [row.name for row in contains_results] == ["Alice", "Charlie"]

    and_results = user_table.find_many(
        where={
            "AND": [
                    {"name": {"STARTS_WITH": "A"}},
                    {"email": {"EQ": "alice@example.com"}},
            ]
        }
    )
    assert [row.name for row in and_results] == ["Alice"]

    or_results = user_table.find_many(where={"OR": [{"name": {"EQ": "Alice"}}, {"name": {"EQ": "Bob"}}]}, order_by={"name": "asc"})
    assert [row.name for row in or_results] == ["Alice", "Bob"]

    in_results = user_table.find_many(where={"id": {"IN": [1, 3]}}, order_by={"id": "asc"})
    assert [row.name for row in in_results] == ["Alice", "Charlie"]

    not_results = user_table.find_many(where={"name": {"NOT": "Alice"}}, order_by={"name": "asc"})
    assert [row.name for row in not_results] == ["Bob", "Charlie"]

    null_results = user_table.find_many(where={"email": None})
    assert [row.name for row in null_results] == ["Bob"]

    client.__class__.close_all()
