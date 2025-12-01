from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import pytest

from dclassql.codegen import generate_client
from dclassql.push import db_push
from dclassql.runtime.backends.lazy import eager
from dclassql.runtime.datasource import open_sqlite_connection


def test_lazy_relations(tmp_path):
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
        client.lazy_user.insert({"id": 2, "name": "Bob"})
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

        users_with_birthday = client.lazy_user.find_many(
            where={"birthday": {"IS_NOT": None}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_with_birthday] == [1]

        users_without_birthday = client.lazy_user.find_many(
            where={"birthday": {"IS": None}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_without_birthday] == [2]

        users_exact_birthday = client.lazy_user.find_many(
            where={"birthday": {"IS": {"date": {"EQ": datetime(1990, 1, 1)}}}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_exact_birthday] == [1]

        users_not_specific_birthday = client.lazy_user.find_many(
            where={"birthday": {"IS_NOT": {"date": {"EQ": datetime(1990, 1, 1)}}}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_not_specific_birthday] == [2]

        users_with_some_address = client.lazy_user.find_many(
            where={"addresses": {"SOME": {"location": {"EQ": "Home"}}}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_with_some_address] == [1]

        users_without_office = client.lazy_user.find_many(
            where={"addresses": {"NONE": {"location": {"EQ": "Office"}}}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_without_office] == [1, 2]

        users_without_any_address = client.lazy_user.find_many(
            where={"addresses": {"NONE": None}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_without_any_address] == [2]

        users_address_every_contains = client.lazy_user.find_many(
            where={"addresses": {"EVERY": {"location": {"CONTAINS": "o"}}}},
            order_by={"id": "asc"},
        )
        assert [user.id for user in users_address_every_contains] == [1, 2]

    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)
