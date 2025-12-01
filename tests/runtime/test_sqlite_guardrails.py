from __future__ import annotations

from pathlib import Path

import pytest

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
    client.__class__.close_all()


def test_include_not_supported(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    prepare_database(db_path)
    _, client = build_client()
    user_table = client.runtime_user

    result = user_table.find_many(include={"anything": True})
    assert result == []
    client.__class__.close_all()
