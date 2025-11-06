from __future__ import annotations

import sys
import types
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from dclassql.codegen import generate_client
from dclassql.push import db_push
from dclassql.runtime.backends.lazy import eager
from dclassql.runtime.datasource import open_sqlite_connection


__datasource__ = {"provider": "sqlite", "url": None}


@dataclass
class RuntimeUser:
    id: int
    name: str
    email: str | None


def _prepare_database(db_path: Path) -> None:
    global __datasource__
    if db_path.exists():
        db_path.unlink()
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"}
    conn = open_sqlite_connection(f"sqlite:///{db_path.as_posix()}")
    try:
        db_push([RuntimeUser], {"sqlite": conn})
        conn.execute("DELETE FROM RuntimeUser")
        conn.commit()
    finally:
        conn.close()


def _build_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    generated_client = namespace["Client"]
    client = generated_client()
    return namespace, client


def test_insert_and_find_roundtrip(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    stored = user_table.insert(InsertModel(id=None, name="Alice", email="alice@example.com"))
    assert stored.id is not None
    assert stored.name == "Alice"
    assert stored.email == "alice@example.com"

    stored_dict = user_table.insert({"id": None, "name": "Bob", "email": None})
    assert stored_dict.name == "Bob"
    assert stored_dict.email is None

    fetched = user_table.find_many(where={"name": "Alice"})
    assert [user.name for user in fetched] == ["Alice"]

    ordered = user_table.find_many(order_by={"name": "desc"})
    assert [user.name for user in ordered] == ["Bob", "Alice"]

    first = user_table.find_first(order_by={"name": "asc"})
    assert first.name == "Alice"
    client.__class__.close_all()


def test_find_returns_distinct_instances(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    user_table.insert(InsertModel(id=None, name="Alice", email=None))

    first = user_table.find_first(order_by={"id": "asc"})
    second = user_table.find_first(order_by={"id": "asc"})

    assert first is not None and second is not None
    assert first.id == second.id
    assert first is not second

    client.__class__.close_all()


def test_backend_raw_queries(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    _, client = _build_client()
    backend = client.runtime_user._backend

    inserted = backend.execute_raw(
        'INSERT INTO "RuntimeUser" (id, name, email) VALUES (?, ?, ?)',
        (1, "Alice", None),
    )
    assert inserted == 1

    rows = backend.query_raw('SELECT id, name, email FROM "RuntimeUser" WHERE id = ?', (1,))
    assert isinstance(rows, list)
    assert rows[0]["name"] == "Alice"

    updated = backend.execute_raw(
        'UPDATE "RuntimeUser" SET name = ? WHERE id = ?',
        ("Alicia", 1),
    )
    assert updated == 1

    total_rows = backend.query_raw('SELECT COUNT(1) as c FROM "RuntimeUser"')
    assert total_rows[0]["c"] == 1

    client.__class__.close_all()


def test_insert_many_utilises_backend(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    rows = [
        InsertModel(id=None, name="Carol", email=None),
        {"id": None, "name": "Dave", "email": "dave@example.com"},
    ]
    inserted = user_table.insert_many(rows, batch_size=1)
    assert [user.name for user in inserted] == ["Carol", "Dave"]

    all_rows = user_table.find_many(order_by={"name": "asc"})
    assert [user.name for user in all_rows] == ["Carol", "Dave"]
    client.__class__.close_all()


def test_insert_many_generates_sequential_ids(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user
    InsertModel = namespace["RuntimeUserInsert"]

    inserted = user_table.insert_many(
        [
            InsertModel(id=None, name="Foo", email=None),
            InsertModel(id=None, name="Bar", email=None),
            InsertModel(id=None, name="Baz", email=None),
        ],
        batch_size=2,
    )

    ids = [user.id for user in inserted]
    assert ids == [1, 2, 3]
    client.__class__.close_all()


def test_backend_thread_local(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    namespace, client = _build_client()
    user_table = client.runtime_user

    r = user_table.insert({'name': "Eve", 'email': None})
    print(f'{r=}')

    def worker() -> int | None:
        other_client = namespace["Client"]()
        try:
            record = other_client.runtime_user.find_first(order_by={"name": "asc"})
            print(f'{record=}')
            return record.id if record else None
        finally:
            other_client.__class__.close_all()

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(worker)
        thread_result = future.result()

    main_record = user_table.find_first(order_by={"name": "asc"})
    print(f'{thread_result=}')
    assert main_record is not None
    assert thread_result is not None
    client.__class__.close_all()


def test_find_many_rejects_unknown_columns(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    _, client = _build_client()
    user_table = client.runtime_user

    with pytest.raises(KeyError):
        user_table.find_many(where={"unknown": "value"})

    with pytest.raises(ValueError):
        user_table.find_many(order_by={"name": "sideways"})
    client.__class__.close_all()


def test_include_not_supported(tmp_path: Path):
    db_path = tmp_path / "runtime.db"
    _prepare_database(db_path)
    _, client = _build_client()
    user_table = client.runtime_user

    result = user_table.find_many(include={"anything": True})
    assert result == []
    client.__class__.close_all()


def test_lazy_relations(tmp_path: Path):
    module_name = "tests.dynamic_relations"
    db_path = tmp_path / "relations.db"
    module = types.ModuleType(module_name)
    setattr(module, "__datasource__", {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"})
    setattr(module, "datetime", datetime)

    @dataclass
    class LazyUser:
        id: int
        name: str
        birthday: 'LazyBirthDay | None'
        addresses: list['LazyAddress']

    LazyUser.__module__ = module_name
    setattr(module, "LazyUser", LazyUser)

    @dataclass
    class LazyBirthDay:
        user_id: int
        user: LazyUser
        date: datetime

        def primary_key(self):
            return self.user_id

        def foreign_key(self):
            yield self.user.id == self.user_id, LazyUser.birthday

    LazyBirthDay.__module__ = module_name
    setattr(module, "LazyBirthDay", LazyBirthDay)

    @dataclass
    class LazyAddress:
        id: int
        user_id: int
        user: LazyUser
        location: str

        def foreign_key(self):
            yield self.user.id == self.user_id, LazyUser.addresses

    LazyAddress.__module__ = module_name
    setattr(module, "LazyAddress", LazyAddress)

    sys.modules[module_name] = module
    generated_module_name = "tests.generated_relations"
    ClientClass: type[Any] | None = None
    try:
        module_generated = generate_client([LazyUser, LazyBirthDay, LazyAddress])
        generated_module = types.ModuleType(generated_module_name)
        namespace = generated_module.__dict__
        namespace["__name__"] = generated_module_name
        sys.modules[generated_module_name] = generated_module
        exec(module_generated.code, namespace)
        ClientClass = cast(type[Any], namespace["Client"])
        with open_sqlite_connection(f"sqlite:///{db_path.as_posix()}") as conn_setup:
            db_push([namespace["LazyUser"], namespace["LazyBirthDay"], namespace["LazyAddress"]], {"sqlite": conn_setup})
        client = ClientClass()

        client.lazy_user.insert({"id": 1, "name": "Alice"})
        client.lazy_birth_day.insert({"user_id": 1, "date": datetime(1990, 1, 1)})
        client.lazy_address.insert({"id": 1, "user_id": 1, "location": "Home"})

        user = client.lazy_user.find_first(order_by={"id": "asc"})
        user_repr = repr(user)
        assert "<LazyRelationList addresses (lazy)>" in user_repr
        assert f"<LazyRelation {LazyBirthDay.__name__} (lazy)>" in user_repr

        birthday_proxy = user.birthday
        assert isinstance(birthday_proxy, LazyBirthDay)
        assert f"<LazyRelation {LazyBirthDay.__name__} (lazy)>" in repr(birthday_proxy)
        assert str(birthday_proxy.date).startswith("1990-01-01")
        birthday_loaded = user.birthday
        assert isinstance(birthday_loaded, LazyBirthDay)
        assert str(birthday_loaded.date).startswith("1990-01-01")

        addresses_proxy = user.addresses
        assert isinstance(addresses_proxy, list)
        assert "<LazyRelationList addresses (lazy)>" in repr(addresses_proxy)
        assert len(addresses_proxy) == 1
        assert isinstance(addresses_proxy[0], LazyAddress)
        assert addresses_proxy[0].location == "Home"

        address = client.lazy_address.find_first(order_by={"id": "asc"})
        assert isinstance(address.user, LazyUser)
        assert address.user.name == "Alice"
        assert address.user is address.user
        user_addresses = address.user.addresses
        assert isinstance(user_addresses, list)
        assert user_addresses and user_addresses[0].location == "Home"

        included = client.lazy_address.find_many(include={"user": True})
        assert included[0].user.name == "Alice"

        user_included = client.lazy_user.find_many(include={"addresses": True, "birthday": True})
        first_user = user_included[0]
        assert len(first_user.addresses) == 1
        assert isinstance(first_user.birthday, LazyBirthDay)
        assert str(first_user.birthday.date).startswith("1990-01-01")

        user_again = client.lazy_user.find_first(order_by={"id": "asc"})
        lazy_birthday_again = user_again.birthday
        resolved_birthday_again = eager(lazy_birthday_again)
        assert isinstance(resolved_birthday_again, LazyBirthDay)
        assert str(resolved_birthday_again.date).startswith("1990-01-01")
        assert eager(resolved_birthday_again) is resolved_birthday_again

        with pytest.raises(TypeError):
            eager(user_again.addresses)

    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)
