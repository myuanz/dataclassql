from __future__ import annotations

import sqlite3
import sys
import tempfile
import types
import math
from collections.abc import Sequence as ABCSequence
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Mapping, NotRequired, Sequence, get_args, get_origin, get_type_hints
from enum import Enum, StrEnum, IntEnum

import pytest

from dclassql.codegen import generate_client
from dclassql.model_inspector import inspect_models
from dclassql.push import db_push
from dclassql.push.sqlite import _build_sqlite_schema
from dclassql.runtime.backends import SQLiteBackend

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///analytics.db",
}

class UserStatus(Enum):
    ACTIVE = "active"
    DISABLED = "disabled"

class UserType(StrEnum):
    ADMIN = "admin"
    MEMBER = "member"
    GUEST = "guest"

class UserVIPLevel(IntEnum):
    LEVEL_1 = 1
    LEVEL_2 = 2
    LEVEL_3 = 3

type OrderSideAlias = Literal["long", "short"]

@dataclass
class Address:
    id: int
    location: str
    user_id: int
    user: 'User'

    def foreign_key(self):
        yield self.user.id == self.user_id, User.addresses


@dataclass
class BirthDay:
    user_id: int
    user: 'User'
    date: datetime

    def primary_key(self):
        return self.user_id

    def foreign_key(self):
        yield self.user.id == self.user_id, User.birthday


@dataclass
class Book:
    id: int
    name: str
    users: list['UserBook']

    def index(self):
        return self.name


@dataclass
class UserBook:
    user_id: int
    book_id: int
    user: 'User'
    book: Book
    created_at: datetime

    def primary_key(self):
        return (self.user_id, self.book_id)

    def index(self):
        yield self.created_at

    def foreign_key(self):
        yield self.user.id == self.user_id, User.books
        yield self.book.id == self.book_id, Book.users


@dataclass
class Composite:
    id1: int
    id2: int
    uniq1: str
    uniq2: str
    uniq3: str
    name: str

    def primary_key(self):
        return self.id1, self.id2

    def unique_index(self):
        yield (self.uniq1, self.uniq2)
        yield self.uniq3


@dataclass
class User:
    id: int
    name: str
    email: str
    last_login: datetime
    status: UserStatus
    type: UserType
    vip_level: UserVIPLevel | None

    birthday: BirthDay | None
    addresses: list[Address]
    books: list[UserBook]

    def index(self):
        yield self.name
        yield self.name, self.email
        yield self.last_login

    def unique_index(self):
        yield self.name, self.email


@dataclass
class AliasDefaultOrder:
    id: int
    side: OrderSideAlias
    limit_price: float = math.nan


@dataclass
class JsonStamp:
    dt: datetime
    idx: int


@dataclass
class JsonOrder:
    id: int
    stamp: JsonStamp
    stamps: list[JsonStamp]


@dataclass
class JsonModelOrder:
    id: int
    name: str


@dataclass
class JsonModelTrade:
    id: int
    order: JsonModelOrder


@dataclass
class RelationCustomer:
    id: int
    orders: list['RelationOrder']


@dataclass
class RelationOrder:
    id: int
    customer_id: int
    customer: RelationCustomer

    def foreign_key(self):
        yield self.customer.id == self.customer_id, RelationCustomer.orders


@dataclass
class OneWayOrderStatus:
    id: int


@dataclass
class OneWayOrder:
    id: int
    status_id: int | None
    status: OneWayOrderStatus

    def foreign_key(self):
        yield self.status.id == self.status_id, None


@dataclass
class InvalidBackrefOrder:
    id: int
    status_id: int | None
    status: OneWayOrderStatus

    def foreign_key(self):
        yield self.status.id == self.status_id, "status"


def test_generate_client_matches_expected_shape() -> None:
    module = generate_client([User, Address, BirthDay, Book, UserBook, Composite])
    code = module.code
    open('./tests/results.py', 'w', encoding='utf-8').write(code)

    assert "class UserWhereDict" in code
    assert "def insert_many" in code
    assert "def find_many" in code
    assert "def find_first" in code

    namespace: dict[str, Any] = {}
    exec(code, namespace)

    assert module.model_names == ('Address', 'BirthDay', 'Book', 'Composite', 'User', 'UserBook')
    assert 'class UserDict' in code
    user_dict = namespace['UserDict']
    dict_hints = get_type_hints(user_dict, globalns=namespace, localns=namespace)
    addresses_hint = dict_hints['addresses']
    assert get_origin(addresses_hint) is list
    assert get_args(addresses_hint) == (namespace['AddressDict'],)
    birthday_hint = dict_hints['birthday']
    assert set(get_args(birthday_hint)) == {namespace['BirthDayDict'], type(None)}

    assert 'DataSourceConfig' in namespace['__all__']
    assert 'UserUpdateDict' in namespace['__all__']

    data_source_config = namespace['DataSourceConfig']
    generated_client = namespace[module.client_class_name]

    include_alias = namespace['TUserIncludeCol']
    assert get_origin(include_alias) is Literal
    assert set(get_args(include_alias)) == {'addresses', 'birthday', 'books'}

    sortable_alias = namespace['TUserSortableCol']
    assert get_origin(sortable_alias) is Literal

    distinct_alias = namespace['TUserDistinctCol']
    assert get_origin(distinct_alias) is Literal

    user_table_cls = namespace['UserTable']
    column_specs = user_table_cls.column_specs
    column_names = tuple(spec.name for spec in column_specs)
    assert set(get_args(sortable_alias)) == set(column_names)
    assert set(get_args(distinct_alias)) == set(column_names)
    expected_ds = data_source_config(url='sqlite:///analytics.db', name=None)
    assert user_table_cls.datasource == expected_ds
    insert_payload = user_table_cls.serialize_insert({
        "id": None,
        "name": "A",
        "email": "a@example.com",
        "last_login": datetime.now(),
        "status": namespace["UserStatus"].ACTIVE,
    })
    assert insert_payload["status"] == namespace["UserStatus"].ACTIVE.value

    assert generated_client.datasource == data_source_config(url='sqlite:///analytics.db', name=None)
    init_hints = get_type_hints(generated_client.__init__, globalns=namespace, localns=namespace)
    assert set(init_hints.keys()) == {"return", "datasource", "echo_sql"}
    assert init_hints["datasource"] is data_source_config
    assert init_hints["echo_sql"] == bool
    assert init_hints["return"] is type(None)

    user_insert_cls = namespace['UserInsert']
    user_insert_dict = namespace['UserInsertDict']
    user_where_dict = namespace['UserWhereDict']
    assert 'StringFilter' in namespace
    assert 'IntFilter' in namespace
    assert 'DateTimeFilter' in namespace
    relation_filter_names = [
        'UserAddressesRelationFilter',
        'UserBirthdayRelationFilter',
        'UserBooksRelationFilter',
    ]
    for filter_name in relation_filter_names:
        assert filter_name in namespace

    backend_protocol = namespace['BackendProtocol']

    insert_field_names = [f.name for f in fields(user_insert_cls)]
    assert insert_field_names == list(column_names)

    user_model_hints = get_type_hints(User)
    insert_hints = get_type_hints(user_insert_cls, globalns=namespace, localns=namespace)
    assert set(get_args(insert_hints['id'])) == {int, type(None)}
    assert insert_hints['email'] == user_model_hints['email']
    assert insert_hints['last_login'] == user_model_hints['last_login']
    assert insert_hints['status'] == user_model_hints['status']
    assert insert_hints['type'] == user_model_hints['type']
    assert insert_hints['vip_level'] == user_model_hints['vip_level']

    insert_dict_hints = get_type_hints(user_insert_dict, globalns=namespace, localns=namespace)
    insert_dict_hints_extras = get_type_hints(user_insert_dict, globalns=namespace, localns=namespace, include_extras=True)
    id_annotation = insert_dict_hints_extras['id']
    assert get_origin(id_annotation) is NotRequired
    assert insert_dict_hints['email'] == user_model_hints['email']
    assert insert_dict_hints['last_login'] == user_model_hints['last_login']

    assert getattr(user_insert_dict, '__total__') is True

    assert "def asdict(value: User, *, relation_policy: RelationPolicy = 'keep') -> UserDict" in module.asdict_stub
    assert getattr(user_where_dict, '__total__') is False
    assert user_insert_dict not in user_where_dict.__mro__

    where_hints = get_type_hints(user_where_dict, globalns=namespace, localns=namespace)
    assert set(insert_dict_hints.keys()).issubset(set(where_hints.keys()))
    logical_keys = {'AND', 'OR', 'NOT'}
    assert logical_keys <= where_hints.keys()
    for relation_name in ('addresses', 'birthday', 'books'):
        assert relation_name in where_hints

    def _flatten_union(tp: Any) -> set[Any]:
        origin = get_origin(tp)
        if origin is None:
            return {tp}
        if origin is Literal:
            return {tp}
        return set().union(*( _flatten_union(arg) for arg in get_args(tp) ))

    for name, expected_hint in insert_dict_hints.items():
        hint = where_hints[name]
        got_args = _flatten_union(hint)
        assert type(None) in got_args
        got_args.discard(type(None))
        expected_args = _flatten_union(expected_hint)
        expected_args.discard(type(None))
        assert expected_args <= got_args

    relation_hint_expectations = {
        'addresses': namespace['UserAddressesRelationFilter'],
        'birthday': namespace['UserBirthdayRelationFilter'],
        'books': namespace['UserBooksRelationFilter'],
    }
    for relation_name, relation_type in relation_hint_expectations.items():
        assert where_hints[relation_name] is relation_type

    insert_hints = get_type_hints(user_table_cls.insert, globalns=namespace, localns=namespace)
    insert_data_type = insert_hints['data']
    assert set(get_args(insert_data_type)) == {user_insert_cls, user_insert_dict, namespace['User']}
    assert insert_hints['return'] is namespace['User']

    insert_many_hints = get_type_hints(user_table_cls.insert_many, globalns=namespace, localns=namespace)
    insert_many_data = insert_many_hints['data']
    assert get_origin(insert_many_data) is ABCSequence
    inner_union = get_args(insert_many_data)[0]
    assert set(get_args(inner_union)) == {user_insert_cls, user_insert_dict, namespace['User']}
    assert insert_many_hints['return'] == list[namespace['User']]
    assert insert_many_hints['batch_size'] == int | None

    table_init_hints = get_type_hints(user_table_cls.__init__, globalns=namespace, localns=namespace)
    assert table_init_hints['backend'] == backend_protocol

    find_many_hints = get_type_hints(user_table_cls.find_many, globalns=namespace, localns=namespace)
    assert find_many_hints['return'] == list[namespace['User']]

    where_union = find_many_hints['where']
    where_args = set(get_args(where_union))
    assert type(None) in where_args
    where_args.remove(type(None))
    (where_dict_type,) = tuple(where_args)
    assert where_dict_type is user_where_dict

    include_union = find_many_hints['include']
    include_args = set(get_args(include_union))
    assert type(None) in include_args
    include_args.remove(type(None))
    (include_dict_type,) = tuple(include_args)
    assert include_dict_type is namespace['UserIncludeDict']
    assert getattr(include_dict_type, '__total__') is False
    include_dict_hints = get_type_hints(include_dict_type, globalns=namespace, localns=namespace)
    assert include_dict_hints == {name: bool for name in get_args(include_alias)}

    order_union = find_many_hints['order_by']
    order_args = set(get_args(order_union))
    assert type(None) in order_args
    order_args.remove(type(None))
    (order_dict_type,) = tuple(order_args)
    assert order_dict_type is namespace['UserOrderByDict']
    assert getattr(order_dict_type, '__total__') is False
    order_dict_hints = get_type_hints(order_dict_type, globalns=namespace, localns=namespace)
    assert set(order_dict_hints.keys()) == set(column_names)
    for annotation in order_dict_hints.values():
        assert get_origin(annotation) is Literal
        assert set(get_args(annotation)) == {'asc', 'desc'}

    distinct_union = find_many_hints['distinct']
    distinct_args = set(get_args(distinct_union))
    assert type(None) in distinct_args
    distinct_args.discard(type(None))
    assert namespace['TUserDistinctCol'] in distinct_args
    seq_arg = next(
        arg
        for arg in distinct_args
        if get_origin(arg) in {Sequence, ABCSequence}
    )
    assert get_args(seq_arg)[0] is namespace['TUserDistinctCol']

    find_first_hints = get_type_hints(user_table_cls.find_first, globalns=namespace, localns=namespace)
    assert find_first_hints['return'] == namespace['User'] | type(None)
    include_union_first = find_first_hints['include']
    include_args_first = set(get_args(include_union_first))
    assert type(None) in include_args_first
    include_args_first.remove(type(None))
    (include_dict_first,) = tuple(include_args_first)
    assert include_dict_first is namespace['UserIncludeDict']

    distinct_union_first = find_first_hints['distinct']
    distinct_args_first = set(get_args(distinct_union_first))
    assert type(None) in distinct_args_first
    distinct_args_first.discard(type(None))
    assert namespace['TUserDistinctCol'] in distinct_args_first

    update_hints = get_type_hints(user_table_cls.update, globalns=namespace, localns=namespace)
    assert update_hints['data'] is namespace['UserUpdateDict']
    assert update_hints['where'] is user_where_dict
    assert update_hints['return'] is namespace['User']

    update_many_hints = get_type_hints(user_table_cls.update_many, globalns=namespace, localns=namespace)
    assert update_many_hints['data'] is namespace['UserUpdateDict']
    um_where_union = update_many_hints['where']
    um_where_args = set(get_args(um_where_union))
    assert type(None) in um_where_args
    um_where_args.remove(type(None))
    (um_where_type,) = tuple(um_where_args)
    assert um_where_type is user_where_dict
    assert set(get_args(update_many_hints['return_records'])) == {False, True}
    assert update_many_hints['return'] == int | list[namespace['User']]

    upsert_hints = get_type_hints(user_table_cls.upsert, globalns=namespace, localns=namespace)
    assert upsert_hints['where'] is namespace['UserUpsertWhereDict']
    assert upsert_hints['update'] is namespace['UserUpdateDict']
    upsert_insert_union = upsert_hints['insert']
    upsert_insert_args = set(get_args(upsert_insert_union))
    assert namespace['UserInsert'] in upsert_insert_args
    assert namespace['UserInsertDict'] in upsert_insert_args
    assert namespace['User'] in upsert_insert_args
    assert upsert_hints['return'] is namespace['User']

    delete_hints = get_type_hints(user_table_cls.delete, globalns=namespace, localns=namespace)
    assert delete_hints['where'] is user_where_dict
    delete_include_union = delete_hints['include']
    delete_include_args = set(get_args(delete_include_union))
    assert type(None) in delete_include_args
    delete_include_args.remove(type(None))
    (delete_include_type,) = tuple(delete_include_args)
    assert delete_include_type is namespace['UserIncludeDict']
    assert delete_hints['return'] == namespace['User'] | type(None)

    delete_many_hints = get_type_hints(user_table_cls.delete_many, globalns=namespace, localns=namespace)
    delete_many_where_union = delete_many_hints['where']
    delete_many_where_args = set(get_args(delete_many_where_union))
    assert type(None) in delete_many_where_args
    delete_many_where_args.remove(type(None))
    (delete_where_dict_type,) = tuple(delete_many_where_args)
    assert delete_where_dict_type is user_where_dict
    delete_many_return_records = delete_many_hints['return_records']
    assert set(get_args(delete_many_return_records)) == {False, True}
    assert delete_many_hints['return'] == int | list[namespace['User']]


def test_generated_client_expands_type_alias_and_nan_default() -> None:
    module = generate_client([AliasDefaultOrder])
    code = module.code
    assert "from tests.test_codegen import AliasDefaultOrder, OrderSideAlias" in code
    assert "side: OrderSideAlias" in code
    assert "AliasDefaultOrder.__dataclass_fields__['limit_price'].default" in code

    namespace: dict[str, Any] = {}
    exec(code, namespace)

    insert_cls = namespace["AliasDefaultOrderInsert"]
    inserted = insert_cls(id=1, side="long")
    assert math.isnan(inserted.limit_price)


def test_generated_client_serializes_unregistered_dataclass_fields_as_json() -> None:
    module = generate_client([JsonOrder])
    code = module.code
    assert "stamp: JsonStamp" in code
    assert "stamps: list[JsonStamp]" in code
    assert "serialize_json_value(data['stamp'])" in code
    assert "deserialize_json_value(row['stamp'], JsonStamp)" in code

    model_info = inspect_models([JsonOrder])["JsonOrder"]
    create_sql, _ = _build_sqlite_schema(model_info)
    assert '"stamp" TEXT NOT NULL' in create_sql
    assert '"stamps" TEXT NOT NULL' in create_sql

    namespace: dict[str, Any] = {}
    exec(code, namespace)
    conn = sqlite3.connect(":memory:")
    try:
        db_push([JsonOrder], conn)
        table = namespace["JsonOrderTable"](SQLiteBackend(conn))
        stored = table.insert(
            {
                "id": None,
                "stamp": JsonStamp(dt=datetime(2026, 1, 2, 3, 4, 5), idx=1),
                "stamps": [
                    JsonStamp(dt=datetime(2026, 1, 2, 3, 4, 5), idx=1),
                    JsonStamp(dt=datetime(2026, 1, 2, 3, 4, 6), idx=2),
                ],
            }
        )
        assert stored.stamp == JsonStamp(dt=datetime(2026, 1, 2, 3, 4, 5), idx=1)
        assert stored.stamps[1] == JsonStamp(dt=datetime(2026, 1, 2, 3, 4, 6), idx=2)

        raw = conn.execute('SELECT stamp, stamps FROM "JsonOrder"').fetchone()
        assert raw[0].startswith('{"dt":"2026-01-02T03:04:05"')

        json_row = conn.execute(
            """
            SELECT
                json_valid(stamp),
                json_valid(stamps),
                json_extract(stamp, '$.dt'),
                json_extract(stamp, '$.idx'),
                json_extract(stamps, '$[1].dt'),
                json_extract(stamps, '$[1].idx')
            FROM "JsonOrder"
            """
        ).fetchone()
        assert tuple(json_row) == (
            1,
            1,
            "2026-01-02T03:04:05",
            1,
            "2026-01-02T03:04:06",
            2,
        )
    finally:
        conn.close()


def test_model_dataclass_field_without_foreign_key_is_json_column() -> None:
    module = generate_client([JsonModelOrder, JsonModelTrade])
    code = module.code
    assert "order: JsonModelOrder" in code
    assert "serialize_json_value(data['order'])" in code
    assert "deserialize_json_value(row['order'], JsonModelOrder)" in code

    trade_info = inspect_models([JsonModelOrder, JsonModelTrade])["JsonModelTrade"]
    assert [(column.name, column.storage_kind) for column in trade_info.columns] == [
        ("id", "scalar"),
        ("order", "json"),
    ]
    assert trade_info.relations == []

    create_sql, _ = _build_sqlite_schema(trade_info)
    assert '"order" TEXT NOT NULL' in create_sql


def test_foreign_key_dataclass_field_stays_relation_not_json_column() -> None:
    module = generate_client([RelationCustomer, RelationOrder])
    code = module.code
    assert "serialize_json_value(data['customer'])" not in code
    assert "RelationSpec(name='customer'" in code

    order_info = inspect_models([RelationCustomer, RelationOrder])["RelationOrder"]
    assert [column.name for column in order_info.columns] == ["id", "customer_id"]
    assert [(relation.name, relation.target, relation.many) for relation in order_info.relations] == [
        ("customer", RelationCustomer, False)
    ]

    customer_info = inspect_models([RelationCustomer, RelationOrder])["RelationCustomer"]
    assert [column.name for column in customer_info.columns] == ["id"]
    assert [(relation.name, relation.target, relation.many) for relation in customer_info.relations] == [
        ("orders", RelationOrder, True)
    ]


def test_foreign_key_accepts_none_backref_for_one_way_relation() -> None:
    model_infos = inspect_models([OneWayOrderStatus, OneWayOrder])

    order_info = model_infos["OneWayOrder"]
    assert [(relation.name, relation.target, relation.many) for relation in order_info.relations] == [
        ("status", OneWayOrderStatus, False)
    ]
    assert len(order_info.foreign_keys) == 1
    foreign_key = order_info.foreign_keys[0]
    assert foreign_key.local_columns == ("status_id",)
    assert foreign_key.remote_model is OneWayOrderStatus
    assert foreign_key.remote_columns == ("id",)
    assert foreign_key.relation_attribute == "status"
    assert foreign_key.backref_attribute is None
    assert model_infos["OneWayOrderStatus"].relations == []


def test_foreign_key_rejects_invalid_backref() -> None:
    with pytest.raises(
        TypeError,
        match="foreign_key backref must be a relation attribute or None",
    ):
        inspect_models([OneWayOrderStatus, InvalidBackrefOrder])


def test_generated_client_rejects_slotted_model_without_weakref_slot() -> None:
    @dataclass(slots=True)
    class SlottedRecord:
        id: int

    with pytest.raises(TypeError, match="weakref_slot=True"):
        generate_client([SlottedRecord])


def test_generated_client_accepts_slotted_model_with_weakref_slot() -> None:
    @dataclass(slots=True, weakref_slot=True)
    class WeakrefSlottedRecord:
        id: int

    module = generate_client([WeakrefSlottedRecord])

    assert "class WeakrefSlottedRecordTable" in module.code


def test_generated_client_rejects_multiple_datasources() -> None:
    module_name_primary = "tests.codegen_primary"
    module_name_secondary = "tests.codegen_secondary"
    primary_db = Path(tempfile.mkstemp(prefix="primary", suffix=".db")[1])
    secondary_db = Path(tempfile.mkstemp(prefix="secondary", suffix=".db")[1])
    primary_module = types.ModuleType(module_name_primary)
    setattr(primary_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{primary_db.as_posix()}",
        "name": "primary",
    })
    secondary_module = types.ModuleType(module_name_secondary)
    setattr(secondary_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{secondary_db.as_posix()}",
        "name": "secondary",
    })
    sys.modules[module_name_primary] = primary_module
    sys.modules[module_name_secondary] = secondary_module

    @dataclass
    class PrimaryUser:
        id: int

    PrimaryUser.__module__ = module_name_primary
    setattr(primary_module, "PrimaryUser", PrimaryUser)

    @dataclass
    class SecondaryUser:
        id: int

    SecondaryUser.__module__ = module_name_secondary
    setattr(secondary_module, "SecondaryUser", SecondaryUser)

    try:
        with pytest.raises(ValueError, match="only use one datasource"):
            generate_client([PrimaryUser, SecondaryUser])
    finally:
        sys.modules.pop(module_name_primary, None)
        sys.modules.pop(module_name_secondary, None)
        primary_db.unlink(missing_ok=True)
        secondary_db.unlink(missing_ok=True)


def test_generated_client_supports_named_single_datasource() -> None:
    module_name = "tests.codegen_named_single"
    db_path = Path(tempfile.mkstemp(prefix="named", suffix=".db")[1])
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{db_path.as_posix()}",
        "name": "analytics",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class NamedUser:
        id: int

    NamedUser.__module__ = module_name
    setattr(model_module, "NamedUser", NamedUser)

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([NamedUser])
        exec(module.code, namespace)

        generated_client = namespace[module.client_class_name]
        data_source_config = namespace["DataSourceConfig"]
        expected_datasource = data_source_config(
            url=f"sqlite:///{db_path.as_posix()}",
            name="analytics",
        )
        assert generated_client.datasource == expected_datasource

        table_cls = namespace["NamedUserTable"]
        assert table_cls.datasource == expected_datasource
        client = generated_client()
        try:
            assert isinstance(client.named_user, table_cls)
            assert isinstance(client.named_user._backend, SQLiteBackend)
            assert client.named_user._backend is client._backend()
        finally:
            client.close()
    finally:
        sys.modules.pop(module_name, None)
        db_path.unlink(missing_ok=True)


def test_generated_client_dynamic_datasource_pushes_and_uses_override_url(tmp_path: Path) -> None:
    module_name = "tests.codegen_dynamic_datasource"
    default_db = tmp_path / "default.db"
    override_db = tmp_path / "override.db"
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{default_db.as_posix()}",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class Person:
        id: int
        name: str

    Person.__module__ = module_name
    setattr(model_module, "Person", Person)

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([Person])
        exec(module.code, namespace)

        client_cls = namespace[module.client_class_name]
        data_source_config = namespace["DataSourceConfig"]
        client = client_cls(
            datasource=data_source_config(
                url=f"sqlite:///{override_db.as_posix()}",
            )
        )
        try:
            client.push_db()
            client.person.insert({"name": "Alice"})
            rows = client.person.find_many()
            assert [row.name for row in rows] == ["Alice"]
        finally:
            client.close()

        assert override_db.exists()
        assert not default_db.exists()
        conn = sqlite3.connect(override_db)
        try:
            count = conn.execute('SELECT COUNT(*) FROM "Person"').fetchone()[0]
        finally:
            conn.close()
        assert count == 1
    finally:
        sys.modules.pop(module_name, None)


def test_generated_client_uses_implicit_id_for_model_without_id(tmp_path: Path) -> None:
    module_name = "tests.codegen_implicit_id"
    db_path = tmp_path / "implicit.db"
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{db_path.as_posix()}",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class Event:
        name: str
        amount: float

    Event.__module__ = module_name
    setattr(model_module, "Event", Event)

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([Event])
        exec(module.code, namespace)

        client_cls = namespace[module.client_class_name]
        insert_cls = namespace["EventInsert"]
        client = client_cls()
        try:
            client.push_db()
            manual = client.event.insert(insert_cls(id=10, name="manual", amount=1.0))
            stored = client.event.insert({"name": "filled", "amount": 2.0})
            rows = client.event.find_many()
            by_id = client.event.find_many(where={"id": 10})
        finally:
            client.close()

        assert manual == Event(name="manual", amount=1.0)
        assert stored == Event(name="filled", amount=2.0)
        assert rows == [Event(name="manual", amount=1.0), Event(name="filled", amount=2.0)]
        assert by_id == [Event(name="manual", amount=1.0)]

        conn = sqlite3.connect(db_path)
        try:
            table_info = conn.execute('PRAGMA table_info("Event")').fetchall()
            records = conn.execute('SELECT id,name,amount FROM "Event"').fetchall()
        finally:
            conn.close()
        assert [(row[1], row[5]) for row in table_info] == [
            ("id", 1),
            ("name", 0),
            ("amount", 0),
        ]
        assert records == [(10, "manual", 1.0), (11, "filled", 2.0)]
    finally:
        sys.modules.pop(module_name, None)


def test_generated_client_push_db_reuses_memory_connection(tmp_path: Path) -> None:
    module_name = "tests.codegen_memory_datasource"
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{(tmp_path / 'default.db').as_posix()}",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class Person:
        id: int
        name: str

    Person.__module__ = module_name
    setattr(model_module, "Person", Person)

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([Person])
        exec(module.code, namespace)

        client_cls = namespace[module.client_class_name]
        data_source_config = namespace["DataSourceConfig"]
        client = client_cls(
            datasource=data_source_config(
                url="sqlite:///:memory:",
            )
        )
        try:
            client.push_db()
            client.person.insert({"name": "Alice"})
            rows = client.person.find_many()
            assert [row.name for row in rows] == ["Alice"]
        finally:
            client.close()
    finally:
        sys.modules.pop(module_name, None)


def test_generated_client_push_db_force_rebuild_controls_rebuild(tmp_path: Path) -> None:
    module_name = "tests.codegen_force_rebuild"
    db_path = tmp_path / "force.db"
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{db_path.as_posix()}",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class Person:
        id: int
        name: str

    Person.__module__ = module_name
    setattr(model_module, "Person", Person)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute('CREATE TABLE "Person" ("id" INTEGER PRIMARY KEY AUTOINCREMENT)')
    finally:
        conn.close()

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([Person])
        exec(module.code, namespace)
        client_cls = namespace[module.client_class_name]
        client = client_cls()

        try:
            with pytest.raises(RuntimeError, match="模型 Person"):
                client.push_db()

            client.push_db(force_rebuild=True)
        finally:
            client.close()

        conn = sqlite3.connect(db_path)
        try:
            columns = conn.execute('PRAGMA table_info("Person")').fetchall()
        finally:
            conn.close()
        assert [name for (_cid, name, *_rest) in columns] == ["id", "name"]
    finally:
        sys.modules.pop(module_name, None)


def test_generated_client_dynamic_datasource_instances_do_not_share_backend(tmp_path: Path) -> None:
    module_name = "tests.codegen_dynamic_datasource_isolation"
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{(tmp_path / 'default.db').as_posix()}",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class Person:
        id: int
        name: str

    Person.__module__ = module_name
    setattr(model_module, "Person", Person)

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([Person])
        exec(module.code, namespace)

        client_cls = namespace[module.client_class_name]
        data_source_config = namespace["DataSourceConfig"]

        db_a = tmp_path / "a.db"
        db_b = tmp_path / "b.db"
        client_a = client_cls(datasource=data_source_config(url=f"sqlite:///{db_a.as_posix()}"))
        client_b = client_cls(datasource=data_source_config(url=f"sqlite:///{db_b.as_posix()}"))
        try:
            client_a.push_db()
            client_b.push_db()
            client_a.person.insert({"name": "Alice"})
            client_b.person.insert({"name": "Bob"})

            assert [row.name for row in client_a.person.find_many()] == ["Alice"]
            assert [row.name for row in client_b.person.find_many()] == ["Bob"]
            assert client_a.person._backend is client_a._backend()
            assert client_b.person._backend is client_b._backend()
            assert client_a.person._backend is not client_b.person._backend
        finally:
            client_a.close()
            client_b.close()
    finally:
        sys.modules.pop(module_name, None)


def test_generated_client_allows_non_identifier_datasource_name(tmp_path: Path) -> None:
    module_name = "tests.codegen_non_identifier_datasource_name"
    db_path = tmp_path / "invalid-name.db"
    model_module = types.ModuleType(module_name)
    setattr(model_module, "__datasource__", {
        "provider": "sqlite",
        "url": f"sqlite:///{db_path.as_posix()}",
        "name": "not-valid",
    })
    sys.modules[module_name] = model_module

    @dataclass
    class Person:
        id: int

    Person.__module__ = module_name
    setattr(model_module, "Person", Person)

    try:
        namespace: dict[str, Any] = {}
        module = generate_client([Person])
        exec(module.code, namespace)

        client_cls = namespace[module.client_class_name]
        assert client_cls.datasource.name == "not-valid"
        client = client_cls()
        try:
            client.push_db()
        finally:
            client.close()
        assert db_path.exists()
    finally:
        sys.modules.pop(module_name, None)


def test_generated_client_package_allows_direct_import(tmp_path: Path) -> None:
    module = generate_client([User], client_class_name="UserModelClient")
    package_dir = tmp_path / "user_model_client"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text(module.init_code, encoding="utf-8")
    (package_dir / "__init__.pyi").write_text(module.init_stub, encoding="utf-8")
    (package_dir / "client.py").write_text(module.code, encoding="utf-8")
    (package_dir / "asdict.pyi").write_text(module.asdict_stub, encoding="utf-8")

    sys.path.insert(0, str(tmp_path))
    try:
        imported = __import__("user_model_client", fromlist=["UserModelClient", "asdict"])
        client_cls = imported.UserModelClient
        typed_asdict = imported.asdict

        assert client_cls.__name__ == "UserModelClient"
        assert callable(typed_asdict)
    finally:
        sys.path.pop(0)
        sys.modules.pop("user_model_client", None)
        sys.modules.pop("user_model_client.client", None)
