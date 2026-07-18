from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import pytest

from dclassql import record_sql
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
        ClientClass = cast(type[Any], namespace[module_generated.client_class_name])
        with open_sqlite_connection(f"sqlite:///{db_path.as_posix()}") as conn_setup:
            db_push(
                [namespace["LazyUserTable"], namespace["LazyBirthDayTable"], namespace["LazyAddressTable"]],
                conn_setup,
                provider="sqlite",
            )
        client = ClientClass()

        client.lazy_user.insert({"id": 1, "name": "Alice"})
        client.lazy_user.insert({"id": 2, "name": "Bob"})
        client.lazy_birth_day.insert({"user_id": 1, "date": datetime(1990, 1, 1)})
        client.lazy_address.insert({"id": 1, "user_id": 1, "location": "Home"})

        with record_sql() as sqls:
            user = client.lazy_user.find_first(order_by={"id": "asc"})
        assert sqls == [('SELECT "id","name" FROM "LazyUser" ORDER BY "id" ASC LIMIT 1;', ())]
        user_repr = repr(user)
        assert "<LazyRelationList addresses (lazy)>" in user_repr
        assert f"<LazyRelation {LazyBirthDay.__name__} (lazy)>" in user_repr

        birthday_proxy = user.birthday
        assert f"<LazyRelation {LazyBirthDay.__name__} (lazy)>" in repr(birthday_proxy)
        with record_sql() as sqls:
            _ = birthday_proxy.date
        assert sqls == [('SELECT "user_id","date" FROM "LazyBirthDay" WHERE "user_id"=? LIMIT 1;', (1,))]
        assert isinstance(birthday_proxy, LazyBirthDay)
        assert str(birthday_proxy.date).startswith("1990-01-01")
        birthday_loaded = user.birthday
        assert isinstance(birthday_loaded, LazyBirthDay)
        assert str(birthday_loaded.date).startswith("1990-01-01")

        addresses_proxy = user.addresses
        assert isinstance(addresses_proxy, list)
        assert "<LazyRelationList addresses (lazy)>" in repr(addresses_proxy)
        with record_sql() as sqls:
            length = len(addresses_proxy)
            first_address = addresses_proxy[0]
        assert sqls == [('SELECT "id","user_id","location" FROM "LazyAddress" WHERE "user_id"=?;', (1,))]
        assert length == 1
        assert isinstance(first_address, LazyAddress)
        assert first_address.location == "Home"

        with record_sql() as sqls:
            address = client.lazy_address.find_first(order_by={"id": "asc"})
            _ = address.user.name
        assert sqls == [
            ('SELECT "id","user_id","location" FROM "LazyAddress" ORDER BY "id" ASC LIMIT 1;', ()),
            ('SELECT "id","name" FROM "LazyUser" WHERE "id"=? LIMIT 1;', (1,)),
        ]
        assert isinstance(address.user, LazyUser)
        assert address.user.name == "Alice"
        assert address.user is address.user
        user_addresses = address.user.addresses
        assert isinstance(user_addresses, list)
        assert user_addresses and user_addresses[0].location == "Home"

        with record_sql() as sqls:
            included = client.lazy_address.find_many(include={"user": True})
        assert sqls == [
            ('SELECT "id","user_id","location" FROM "LazyAddress";', ()),
            ('SELECT "id","name" FROM "LazyUser" WHERE "id"=? LIMIT 1;', (1,)),
        ]
        assert included[0].user.name == "Alice"

        with record_sql() as sqls:
            user_included = client.lazy_user.find_many(include={"addresses": True, "birthday": True})
        assert sqls == [
            ('SELECT "id","name" FROM "LazyUser";', ()),
            ('SELECT "user_id","date" FROM "LazyBirthDay" WHERE "user_id"=? LIMIT 1;', (1,)),
            ('SELECT "id","user_id","location" FROM "LazyAddress" WHERE "user_id"=?;', (1,)),
            ('SELECT "user_id","date" FROM "LazyBirthDay" WHERE "user_id"=? LIMIT 1;', (2,)),
            ('SELECT "id","user_id","location" FROM "LazyAddress" WHERE "user_id"=?;', (2,)),
        ]
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

        with record_sql() as sqls:
            users_with_birthday = client.lazy_user.find_many(
                where={"birthday": {"IS_NOT": None}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="LazyUser"."id") ORDER BY "id" ASC;',
                (),
            )
        ]
        assert [user.id for user in users_with_birthday] == [1]

        with record_sql() as sqls:
            users_without_birthday = client.lazy_user.find_many(
                where={"birthday": {"IS": None}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE NOT EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="LazyUser"."id") ORDER BY "id" ASC;',
                (),
            )
        ]
        assert [user.id for user in users_without_birthday] == [2]

        with record_sql() as sqls:
            users_exact_birthday = client.lazy_user.find_many(
                where={"birthday": {"IS": {"date": {"EQ": datetime(1990, 1, 1)}}}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="LazyUser"."id" AND "LazyBirthDay"."date"=?) ORDER BY "id" ASC;',
                (datetime(1990, 1, 1),),
            )
        ]
        assert [user.id for user in users_exact_birthday] == [1]

        with record_sql() as sqls:
            users_not_specific_birthday = client.lazy_user.find_many(
                where={"birthday": {"IS_NOT": {"date": {"EQ": datetime(1990, 1, 1)}}}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE NOT EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="LazyUser"."id" AND "LazyBirthDay"."date"=?) ORDER BY "id" ASC;',
                (datetime(1990, 1, 1),),
            )
        ]
        assert [user.id for user in users_not_specific_birthday] == [2]

        with record_sql() as sqls:
            users_with_some_address = client.lazy_user.find_many(
                where={"addresses": {"SOME": {"location": {"EQ": "Home"}}}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="LazyUser"."id" AND "LazyAddress"."location"=?) ORDER BY "id" ASC;',
                ("Home",),
            )
        ]
        assert [user.id for user in users_with_some_address] == [1]

        with record_sql() as sqls:
            users_without_office = client.lazy_user.find_many(
                where={"addresses": {"NONE": {"location": {"EQ": "Office"}}}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE NOT EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="LazyUser"."id" AND "LazyAddress"."location"=?) ORDER BY "id" ASC;',
                ("Office",),
            )
        ]
        assert [user.id for user in users_without_office] == [1, 2]

        with record_sql() as sqls:
            users_without_any_address = client.lazy_user.find_many(
                where={"addresses": {"NONE": None}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE NOT EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="LazyUser"."id") ORDER BY "id" ASC;',
                (),
            )
        ]
        assert [user.id for user in users_without_any_address] == [2]

        with record_sql() as sqls:
            users_address_every_contains = client.lazy_user.find_many(
                where={"addresses": {"EVERY": {"location": {"CONTAINS": "o"}}}},
                order_by={"id": "asc"},
            )
        assert sqls == [
            (
                'SELECT "id","name" FROM "LazyUser" WHERE NOT EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="LazyUser"."id" AND "LazyAddress"."location" NOT LIKE ? ESCAPE \'\\\') ORDER BY "id" ASC;',
                ("%o%",),
            )
        ]
        assert [user.id for user in users_address_every_contains] == [1, 2]

    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)


def test_lazy_descriptor_preserves_plain_slotted_instance_relation(tmp_path):
    module_name = "tests.dynamic_slotted_plain_relation"
    db_path = tmp_path / "slotted_plain_relation.db"
    module = types.ModuleType(module_name)
    setattr(module, "__datasource__", {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"})

    @dataclass(slots=True, weakref_slot=True)
    class SlottedOrder:
        id: int
        symbol: str
        trades: list['SlottedTrade']

    SlottedOrder.__module__ = module_name
    setattr(module, "SlottedOrder", SlottedOrder)

    @dataclass(slots=True, weakref_slot=True)
    class SlottedTrade:
        order: SlottedOrder
        order_id: int
        pnl: float

        def foreign_key(self):
            yield self.order.id == self.order_id, SlottedOrder.trades

    SlottedTrade.__module__ = module_name
    setattr(module, "SlottedTrade", SlottedTrade)

    sys.modules[module_name] = module
    generated_module_name = "tests.generated_slotted_plain_relation"
    ClientClass: type[Any] | None = None
    try:
        module_generated = generate_client([SlottedOrder, SlottedTrade])
        generated_module = types.ModuleType(generated_module_name)
        namespace = generated_module.__dict__
        namespace["__name__"] = generated_module_name
        sys.modules[generated_module_name] = generated_module
        exec(module_generated.code, namespace)
        ClientClass = cast(type[Any], namespace[module_generated.client_class_name])
        with open_sqlite_connection(f"sqlite:///{db_path.as_posix()}") as conn_setup:
            db_push(
                [namespace["SlottedOrderTable"], namespace["SlottedTradeTable"]],
                conn_setup,
                provider="sqlite",
            )
        client = ClientClass()

        client.slotted_order.insert({"id": 1, "symbol": "rb"})
        client.slotted_trade.insert({"order_id": 1, "pnl": 1.0})
        assert client.slotted_trade.find_first() is not None

        order = SlottedOrder(id=2, symbol="ag", trades=[])
        plain_trade = SlottedTrade(order=order, order_id=2, pnl=2.0)

        assert plain_trade.order is order
    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)


def test_trade_entry_and_exit_order_relations(tmp_path):
    module_name = "tests.dynamic_trade_relations"
    db_path = tmp_path / "trade_relations.db"
    module = types.ModuleType(module_name)
    setattr(module, "__datasource__", {"url": f"sqlite:///{db_path.as_posix()}"})

    @dataclass
    class TradeOrder:
        id: int
        symbol: str
        entry_trades: list['Trade']
        exit_trades: list['Trade']

    TradeOrder.__module__ = module_name
    setattr(module, "TradeOrder", TradeOrder)

    @dataclass
    class Trade:
        id: int
        symbol: str
        entry_order_id: int
        exit_order_id: int | None
        entry_order: TradeOrder
        exit_order: TradeOrder | None

        def foreign_key(self):
            yield self.entry_order.id == self.entry_order_id, TradeOrder.entry_trades
            yield self.exit_order and self.exit_order.id == self.exit_order_id, TradeOrder.exit_trades

    Trade.__module__ = module_name
    setattr(module, "Trade", Trade)

    sys.modules[module_name] = module
    generated_module_name = "tests.generated_trade_relations"
    ClientClass: type[Any] | None = None
    try:
        module_generated = generate_client([TradeOrder, Trade])
        generated_module = types.ModuleType(generated_module_name)
        namespace = generated_module.__dict__
        namespace["__name__"] = generated_module_name
        sys.modules[generated_module_name] = generated_module
        exec(module_generated.code, namespace)
        ClientClass = cast(type[Any], namespace[module_generated.client_class_name])
        with open_sqlite_connection(f"sqlite:///{db_path.as_posix()}") as conn_setup:
            db_push(
                [namespace["TradeOrderTable"], namespace["TradeTable"]],
                conn_setup,
                provider="sqlite",
            )
        client = ClientClass()

        client.trade_order.insert({"id": 1, "symbol": "rb"})
        client.trade_order.insert({"id": 2, "symbol": "rb"})
        client.trade.insert(
            {
                "id": 10,
                "symbol": "rb",
                "entry_order_id": 1,
                "exit_order_id": 2,
            }
        )

        trade = client.trade.find_first(
            where={"id": {"EQ": 10}},
            include={"entry_order": True, "exit_order": True},
        )
        assert trade is not None
        assert isinstance(trade.entry_order, TradeOrder)
        assert isinstance(trade.exit_order, TradeOrder)
        assert trade.entry_order.id == 1
        assert trade.exit_order.id == 2

        entry_order = client.trade_order.find_first(
            where={"id": {"EQ": 1}},
            include={"entry_trades": True, "exit_trades": True},
        )
        assert entry_order is not None
        assert [item.id for item in entry_order.entry_trades] == [10]
        assert entry_order.exit_trades == []

        exit_order = client.trade_order.find_first(
            where={"id": {"EQ": 2}},
            include={"entry_trades": True, "exit_trades": True},
        )
        assert exit_order is not None
        assert exit_order.entry_trades == []
        assert [item.id for item in exit_order.exit_trades] == [10]

    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)
