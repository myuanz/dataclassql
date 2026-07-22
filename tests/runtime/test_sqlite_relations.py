from __future__ import annotations

import sys
import types
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import pytest

from dclassql import record_sql
from dclassql.codegen import generate_client
from dclassql.model_inspector import DataSourceConfig
from dclassql.push import db_push
from dclassql.runtime.backends import LazyLookupKey, LazyRelationView
from dclassql.runtime.backends.lazy import eager
from dclassql.runtime.datasource import open_sqlite_connection

__datasource__ = {"url": "sqlite:///:memory:"}


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
        assert sqls == [('SELECT "t"."id","t"."name" FROM "LazyUser" "t" ORDER BY "t"."id" ASC LIMIT 1;', ())]
        user_repr = repr(user)
        assert "<LazyRelationView addresses (lazy)>" in user_repr
        assert f"<LazyRelation {LazyBirthDay.__name__} (lazy)>" in user_repr

        birthday_proxy = user.birthday
        assert f"<LazyRelation {LazyBirthDay.__name__} (lazy)>" in repr(birthday_proxy)
        with record_sql() as sqls:
            birthday_date = birthday_proxy.date
            birthday_date_again = birthday_proxy.date
        assert sqls == [('SELECT "user_id","date" FROM "LazyBirthDay" WHERE "user_id"=? LIMIT 1;', (1,))]
        assert isinstance(birthday_proxy, LazyBirthDay)
        assert birthday_date == birthday_date_again
        assert str(birthday_date).startswith("1990-01-01")

        birthday_again = user.birthday
        assert birthday_again is not birthday_proxy
        with record_sql() as sqls:
            assert str(birthday_again.date).startswith("1990-01-01")
        assert sqls == [('SELECT "user_id","date" FROM "LazyBirthDay" WHERE "user_id"=? LIMIT 1;', (1,))]

        addresses_proxy = user.addresses
        assert isinstance(addresses_proxy, LazyRelationView)
        assert "<LazyRelationView addresses (lazy)>" in repr(addresses_proxy)
        with record_sql() as sqls:
            length = len(addresses_proxy)
            first_address = addresses_proxy[0]
        assert sqls == [
            (
                'SELECT COUNT(*) "__count" FROM "LazyAddress" WHERE "user_id"=?;',
                (1,),
            ),
            (
                'SELECT "id","user_id","location" FROM "LazyAddress" '
                'WHERE "user_id"=? LIMIT 1;',
                (1,),
            ),
        ]
        assert length == 1
        assert isinstance(first_address, LazyAddress)
        assert first_address.location == "Home"
        assert user.addresses is not user.addresses

        with record_sql() as sqls:
            address = client.lazy_address.find_first(order_by={"id": "asc"})
            related_user = address.user
            _ = related_user.name
            _ = related_user.name
        assert sqls == [
            ('SELECT "t"."id","t"."user_id","t"."location" FROM "LazyAddress" "t" ORDER BY "t"."id" ASC LIMIT 1;', ()),
            ('SELECT "id","name" FROM "LazyUser" WHERE "id"=? LIMIT 1;', (1,)),
        ]
        assert isinstance(related_user, LazyUser)
        assert related_user.name == "Alice"
        assert address.user is not address.user
        resolved_user = eager(related_user)
        user_addresses = resolved_user.addresses
        assert isinstance(user_addresses, LazyRelationView)
        assert user_addresses and user_addresses[0].location == "Home"

        with record_sql() as sqls:
            included = client.lazy_address.find_many(include={"user": True})
        assert sqls == [
            ('SELECT "id","user_id","location" FROM "LazyAddress";', ()),
            ('SELECT "id","name" FROM "LazyUser" WHERE "id"=? LIMIT 1;', (1,)),
        ]
        with record_sql() as sqls:
            assert included[0].user.name == "Alice"
            assert included[0].user is included[0].user
        assert sqls == []

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
        assert type(first_user.addresses) is list
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="t"."id") ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE NOT EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="t"."id") ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="t"."id" AND "LazyBirthDay"."date"=?) ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE NOT EXISTS (SELECT 1 FROM "LazyBirthDay" WHERE "LazyBirthDay"."user_id"="t"."id" AND "LazyBirthDay"."date"=?) ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="t"."id" AND "LazyAddress"."location"=?) ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE NOT EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="t"."id" AND "LazyAddress"."location"=?) ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE NOT EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="t"."id") ORDER BY "t"."id" ASC;',
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
                'SELECT "t"."id","t"."name" FROM "LazyUser" "t" WHERE NOT EXISTS (SELECT 1 FROM "LazyAddress" WHERE "LazyAddress"."user_id"="t"."id" AND "LazyAddress"."location" NOT LIKE ? ESCAPE \'\\\') ORDER BY "t"."id" ASC;',
                ("%o%",),
            )
        ]
        assert [user.id for user in users_address_every_contains] == [1, 2]

        second_user = user_included[1]
        assert not second_user.addresses
        client.lazy_address.insert({"id": 2, "user_id": 2, "location": "Office"})
        with record_sql() as sqls:
            assert not second_user.addresses
        assert sqls == []

        fresh_second_user = client.lazy_user.find_first(where={"id": 2})
        assert fresh_second_user is not None
        with record_sql() as sqls:
            assert len(fresh_second_user.addresses) == 1
        assert sqls == [
            (
                'SELECT COUNT(*) "__count" FROM "LazyAddress" WHERE "user_id"=?;',
                (2,),
            )
        ]

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
        assert not entry_order.exit_trades

        exit_order = client.trade_order.find_first(
            where={"id": {"EQ": 2}},
            include={"entry_trades": True, "exit_trades": True},
        )
        assert exit_order is not None
        assert not exit_order.entry_trades
        assert [item.id for item in exit_order.exit_trades] == [10]

    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)


def test_explicit_join_model_many_to_many_relations(tmp_path):
    module_name = "tests.dynamic_many_to_many_relations"
    generated_module_name = "tests.generated_many_to_many_relations"
    db_path = tmp_path / "many_to_many_relations.db"
    module = types.ModuleType(module_name)
    setattr(module, "__datasource__", {"url": f"sqlite:///{db_path.as_posix()}"})

    @dataclass
    class ManyUser:
        id: int
        name: str
        books: list['ManyUserBook']

    ManyUser.__module__ = module_name
    setattr(module, "ManyUser", ManyUser)

    @dataclass
    class ManyBook:
        id: int
        title: str
        users: list['ManyUserBook']

    ManyBook.__module__ = module_name
    setattr(module, "ManyBook", ManyBook)

    @dataclass
    class ManyUserBook:
        user_id: int
        book_id: int
        user: ManyUser
        book: ManyBook

        def primary_key(self):
            return self.user_id, self.book_id

        def foreign_key(self):
            yield self.user.id == self.user_id, ManyUser.books
            yield self.book.id == self.book_id, ManyBook.users

    ManyUserBook.__module__ = module_name
    setattr(module, "ManyUserBook", ManyUserBook)

    sys.modules[module_name] = module
    ClientClass: type[Any] | None = None
    try:
        generated = generate_client([ManyUser, ManyBook, ManyUserBook])
        generated_module = types.ModuleType(generated_module_name)
        namespace = generated_module.__dict__
        namespace["__name__"] = generated_module_name
        sys.modules[generated_module_name] = generated_module
        exec(generated.code, namespace)
        ClientClass = cast(type[Any], namespace[generated.client_class_name])

        with open_sqlite_connection(f"sqlite:///{db_path.as_posix()}") as connection:
            db_push(
                [
                    namespace["ManyUserTable"],
                    namespace["ManyBookTable"],
                    namespace["ManyUserBookTable"],
                ],
                connection,
                provider="sqlite",
            )

        client = ClientClass()
        client.many_user.insert({"id": 1, "name": "Alice"})
        client.many_user.insert({"id": 2, "name": "Bob"})
        client.many_book.insert({"id": 1, "title": "Database"})
        client.many_book.insert({"id": 2, "title": "Python"})
        client.many_user_book.insert({"user_id": 1, "book_id": 1})
        client.many_user_book.insert({"user_id": 1, "book_id": 2})

        alice = client.many_user.find_first(
            where={"id": 1},
            include={"books": True},
        )
        assert alice is not None
        assert sorted(link.book.title for link in alice.books) == ["Database", "Python"]

        database = client.many_book.find_first(
            where={"id": 1},
            include={"users": True},
        )
        assert database is not None
        assert [link.user.name for link in database.users] == ["Alice"]

        python_readers = client.many_user.find_many(
            where={
                "books": {
                    "SOME": {
                        "book": {
                            "IS": {"title": "Python"},
                        },
                    },
                },
            },
        )
        assert [user.name for user in python_readers] == ["Alice"]

        bob = client.many_user.find_first(where={"id": 2}, include={"books": True})
        python = client.many_book.find_first(where={"id": 2}, include={"users": True})
        assert bob is not None
        assert python is not None
        assert not bob.books
        assert [link.user_id for link in python.users] == [1]

        client.many_user_book.insert({"user_id": 2, "book_id": 2})

        assert not bob.books
        assert [link.user_id for link in python.users] == [1]

        fresh_bob = client.many_user.find_first(where={"id": 2})
        fresh_python = client.many_book.find_first(where={"id": 2})
        assert fresh_bob is not None and fresh_python is not None
        assert [link.book_id for link in fresh_bob.books] == [2]
        assert [link.user_id for link in fresh_python.users] == [1, 2]
    finally:
        if ClientClass is not None:
            ClientClass.close_all()
        sys.modules.pop(generated_module_name, None)
        sys.modules.pop(module_name, None)


@dataclass
class CachedParent:
    id: int
    name: str
    children: list['CachedChild']


@dataclass
class CachedChild:
    id: int
    parent_id: int
    parent: CachedParent

    def foreign_key(self):
        yield self.parent.id == self.parent_id, CachedParent.children


def test_lazy_relation_view_scope_and_include_list(tmp_path):
    generated = generate_client([CachedParent, CachedChild])
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    client = namespace[generated.client_class_name](
        datasource=DataSourceConfig(url=f"sqlite:///{(tmp_path / 'cache.db').as_posix()}")
    )

    try:
        client.push_db()
        client.cached_parent.insert({"id": 1, "name": "one"})
        client.cached_child.insert({"id": 1, "parent_id": 1})

        parent = client.cached_parent.find_first(where={"id": 1})
        assert parent is not None
        children = parent.children
        with record_sql() as sqls:
            assert [child.id for child in children] == [1]
            assert [child.id for child in children] == [1]
        assert sqls == [
            ('SELECT "id","parent_id" FROM "CachedChild" WHERE "parent_id"=?;', (1,)),
        ]

        client.cached_child.insert({"id": 2, "parent_id": 1})
        with record_sql() as sqls:
            assert [child.id for child in children] == [1]
        assert sqls == []

        with record_sql() as sqls:
            assert [child.id for child in parent.children] == [1, 2]
        assert sqls == [
            ('SELECT "id","parent_id" FROM "CachedChild" WHERE "parent_id"=?;', (1,)),
        ]

        included_parent = client.cached_parent.find_first(
            where={"id": 1}, include={"children": True}
        )
        assert included_parent is not None
        assert type(included_parent.children) is list
        assert [child.id for child in included_parent.children] == [1, 2]

        client.cached_child.insert({"id": 3, "parent_id": 1})
        with record_sql() as sqls:
            assert [child.id for child in included_parent.children] == [1, 2]
        assert sqls == []

        with record_sql() as sqls:
            assert [child.id for child in parent.children] == [1, 2, 3]
        assert sqls == [
            ('SELECT "id","parent_id" FROM "CachedChild" WHERE "parent_id"=?;', (1,)),
        ]
    finally:
        client.close()


def test_lazy_relation_view_reads_and_lookup_identity(tmp_path):
    generated = generate_client([CachedParent, CachedChild])
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    client = namespace[generated.client_class_name](
        datasource=DataSourceConfig(url=f"sqlite:///{(tmp_path / 'view.db').as_posix()}")
    )

    try:
        client.push_db()
        client.cached_parent.insert({"id": 1, "name": "one"})
        client.cached_child.insert({"id": 1, "parent_id": 1})
        client.cached_child.insert({"id": 2, "parent_id": 1})

        parent = client.cached_parent.find_first(where={"id": 1})
        same_parent = client.cached_parent.find_first(where={"id": 1})
        assert parent is not None
        assert same_parent is not None
        view = parent.children
        same_source = parent.children

        assert type(view) is LazyRelationView
        assert isinstance(view.lookup_key, LazyLookupKey)
        assert view.lookup_key == same_source.lookup_key
        assert hash(view) == hash(same_source)
        with record_sql() as sqls:
            assert view == same_source
            assert parent == same_parent
        assert sqls == []
        with pytest.raises(TypeError, match="list"):
            _ = view == []
        with pytest.raises(AttributeError, match="append"):
            cast(Any, view).append(view[0])

        with record_sql() as sqls:
            assert bool(view)
        assert sqls == [
            (
                'SELECT "id","parent_id" FROM "CachedChild" '
                'WHERE "parent_id"=? LIMIT 1;',
                (1,),
            )
        ]

        with record_sql() as sqls:
            assert len(view) == 2
        assert sqls == [
            (
                'SELECT COUNT(*) "__count" FROM "CachedChild" '
                'WHERE "parent_id"=?;',
                (1,),
            )
        ]

        with record_sql() as sqls:
            assert view[1].id == 2
        assert sqls == [
            (
                'SELECT "id","parent_id" FROM "CachedChild" '
                'WHERE "parent_id"=? LIMIT 1 OFFSET 1;',
                (1,),
            )
        ]

        with record_sql() as sqls:
            assert [child.id for child in view] == [1, 2]
            assert list(view)[1].id == 2
        assert sqls == [
            (
                'SELECT "id","parent_id" FROM "CachedChild" '
                'WHERE "parent_id"=?;',
                (1,),
            )
        ]

        included_parent = client.cached_parent.find_first(
            where={"id": 1}, include={"children": True}
        )
        assert included_parent is not None
        included_children = included_parent.children
        assert type(included_children) is list
        with record_sql() as sqls:
            assert len(included_children) == 2
            assert included_children[0].id == 1
        assert sqls == []
    finally:
        client.close()


@dataclass
class FreshStatus:
    id: int
    code: str
    name: str

    def unique_index(self):
        return self.code


@dataclass
class FreshOrder:
    id: int
    status_code: str
    status: FreshStatus | None

    def foreign_key(self):
        yield self.status and self.status.code == self.status_code, None


def test_lazy_relations_retry_after_query_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generated = generate_client([
        CachedParent,
        CachedChild,
        FreshStatus,
        FreshOrder,
    ])
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    client = namespace[generated.client_class_name](
        datasource=DataSourceConfig(
            url=f"sqlite:///{(tmp_path / 'relation-retry.db').as_posix()}"
        )
    )

    try:
        client.push_db()
        client.cached_parent.insert({"id": 1, "name": "one"})
        client.cached_child.insert({"id": 10, "parent_id": 1})
        client.fresh_status.insert({"id": 1, "code": "ready", "name": "Ready"})
        client.fresh_order.insert({"id": 1, "status_code": "ready"})

        parent = client.cached_parent.find_first(where={"id": 1})
        order = client.fresh_order.find_first(where={"id": 1})
        assert parent is not None and order is not None
        children_view = parent.children
        status_proxy = order.status
        assert status_proxy is not None

        backend = client.cached_parent._backend
        original_query_raw = backend.query_raw
        attempts = {"CachedChild": 0, "FreshStatus": 0}

        def fail_first_relation_query(
            sql: str,
            params: Sequence[object] | None = None,
            auto_commit: bool = False,
        ) -> Sequence[dict[str, object]]:
            for table_name in attempts:
                if f'FROM "{table_name}"' not in sql:
                    continue
                attempts[table_name] += 1
                if attempts[table_name] == 1:
                    raise RuntimeError(f"temporary {table_name} query failure")
            return original_query_raw(sql, params, auto_commit=auto_commit)

        monkeypatch.setattr(backend, "query_raw", fail_first_relation_query)

        with pytest.raises(RuntimeError, match="temporary CachedChild query failure"):
            list(children_view)
        assert [child.id for child in children_view] == [10]
        assert attempts["CachedChild"] == 2

        with pytest.raises(RuntimeError, match="temporary FreshStatus query failure"):
            _ = status_proxy.name
        assert status_proxy.name == "Ready"
        assert attempts["FreshStatus"] == 2
    finally:
        client.close()


def test_lazy_single_proxy_scope_and_include_snapshot(tmp_path):
    generated = generate_client([FreshStatus, FreshOrder])
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    client = namespace[generated.client_class_name](
        datasource=DataSourceConfig(
            url=f"sqlite:///{(tmp_path / 'single-relation.db').as_posix()}"
        )
    )

    try:
        client.push_db()
        client.fresh_order.insert({"id": 1, "status_code": "ready"})
        order = client.fresh_order.find_first(where={"id": 1})
        same_order = client.fresh_order.find_first(where={"id": 1})
        assert order is not None
        assert same_order is not None
        with record_sql() as sqls:
            assert order.status == same_order.status
            assert hash(order.status) == hash(same_order.status)
            assert order == same_order
        assert sqls == []

        missing_status = order.status
        assert missing_status is not None
        with record_sql() as sqls:
            assert not missing_status
        assert sqls == [
            (
                'SELECT "id","code","name" FROM "FreshStatus" '
                'WHERE "code"=? LIMIT 1;',
                ("ready",),
            )
        ]

        client.fresh_status.insert({"id": 1, "code": "ready", "name": "Ready"})
        with record_sql() as sqls:
            assert not missing_status
        assert sqls == []

        fresh_status = order.status
        assert fresh_status is not None
        with record_sql() as sqls:
            assert fresh_status.name == "Ready"
        assert sqls == [
            (
                'SELECT "id","code","name" FROM "FreshStatus" '
                'WHERE "code"=? LIMIT 1;',
                ("ready",),
            )
        ]

        included_order = client.fresh_order.find_first(
            where={"id": 1}, include={"status": True}
        )
        assert included_order is not None
        included_status = included_order.status
        assert included_status is not None
        with pytest.raises(TypeError, match="eager"):
            _ = fresh_status == included_status

        client.fresh_status.update(
            data={"name": "Completed"},
            where={"id": 1},
        )
        with record_sql() as sqls:
            assert fresh_status.name == "Ready"
            assert included_status.name == "Ready"
        assert sqls == []

        current_status = order.status
        assert current_status is not None
        with record_sql() as sqls:
            assert current_status.name == "Completed"
        assert sqls == [
            (
                'SELECT "id","code","name" FROM "FreshStatus" '
                'WHERE "code"=? LIMIT 1;',
                ("ready",),
            )
        ]
    finally:
        client.close()


@dataclass
class RuntimeSelfNode:
    id: int
    parent_id: int | None
    parent: 'RuntimeSelfNode | None'
    children: list['RuntimeSelfNode']

    def foreign_key(self):
        yield self.parent and self.parent.id == self.parent_id, RuntimeSelfNode.children


def test_self_relation_round_trip(tmp_path):
    generated = generate_client([RuntimeSelfNode])
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    client = namespace[generated.client_class_name](
        datasource=DataSourceConfig(
            url=f"sqlite:///{(tmp_path / 'self-relation.db').as_posix()}"
        )
    )

    try:
        client.push_db()
        client.runtime_self_node.insert({"id": 1, "parent_id": None})
        client.runtime_self_node.insert({"id": 2, "parent_id": 1})

        root = client.runtime_self_node.find_first(
            where={"id": 1}, include={"parent": True, "children": True}
        )
        child = client.runtime_self_node.find_first(
            where={"id": 2}, include={"parent": True, "children": True}
        )

        assert root is not None and child is not None
        assert root.parent is None
        assert [node.id for node in root.children] == [2]
        assert child.parent is not None
        assert child.parent.id == 1
        assert not child.children
    finally:
        client.close()


@dataclass
class RuntimeCompositeParent:
    tenant_id: int
    id: int
    name: str
    children: list['RuntimeCompositeChild']

    def primary_key(self):
        return self.tenant_id, self.id


@dataclass
class RuntimeCompositeChild:
    id: int
    tenant_id: int
    parent_id: int
    parent: RuntimeCompositeParent

    def foreign_key(self):
        yield (
            self.parent.tenant_id == self.tenant_id,
            self.parent.id == self.parent_id,
        ), RuntimeCompositeParent.children


def test_composite_relation_round_trip_and_filter(tmp_path):
    generated = generate_client([RuntimeCompositeParent, RuntimeCompositeChild])
    namespace: dict[str, Any] = {}
    exec(generated.code, namespace)
    client = namespace[generated.client_class_name](
        datasource=DataSourceConfig(
            url=f"sqlite:///{(tmp_path / 'composite-relation.db').as_posix()}"
        )
    )

    try:
        client.push_db()
        client.runtime_composite_parent.insert(
            {"tenant_id": 1, "id": 10, "name": "one"}
        )
        client.runtime_composite_parent.insert(
            {"tenant_id": 2, "id": 10, "name": "two"}
        )
        client.runtime_composite_child.insert(
            {"id": 1, "tenant_id": 1, "parent_id": 10}
        )

        child = client.runtime_composite_child.find_first(
            where={"id": 1}, include={"parent": True}
        )
        parent_one = client.runtime_composite_parent.find_first(
            where={"tenant_id": 1, "id": 10}, include={"children": True}
        )
        parent_two = client.runtime_composite_parent.find_first(
            where={"tenant_id": 2, "id": 10}, include={"children": True}
        )
        filtered = client.runtime_composite_child.find_many(
            where={"parent": {"IS": {"name": "one"}}}
        )

        assert child is not None
        assert parent_one is not None and parent_two is not None
        assert (child.parent.tenant_id, child.parent.id) == (1, 10)
        assert [item.id for item in parent_one.children] == [1]
        assert not parent_two.children
        assert [item.id for item in filtered] == [1]
    finally:
        client.close()
