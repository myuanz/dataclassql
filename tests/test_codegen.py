from __future__ import annotations

import importlib
import sqlite3
import sys
import tempfile
import types
from collections.abc import Sequence as ABCSequence
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Mapping, NotRequired, get_args, get_origin, get_type_hints
from enum import Enum, StrEnum, IntEnum

from dclassql.cli import compute_model_target, resolve_generated_path
from dclassql.codegen import generate_client
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


def test_generate_client_matches_expected_shape() -> None:
    module = generate_client([User, Address, BirthDay, Book, UserBook])
    code = module.code
    open('./tests/results.py', 'w', encoding='utf-8').write(code)

    assert "class UserWhereDict" in code
    assert "def insert_many" in code
    assert "def find_many" in code
    assert "def find_first" in code

    namespace: dict[str, Any] = {}
    exec(code, namespace)

    assert module.model_names == ('Address', 'BirthDay', 'Book', 'User', 'UserBook')

    assert 'DataSourceConfig' in namespace['__all__']

    data_source_config = namespace['DataSourceConfig']
    generated_client = namespace['Client']

    include_alias = namespace['TUserIncludeCol']
    assert get_origin(include_alias) is Literal
    assert set(get_args(include_alias)) == {'addresses', 'birthday', 'books'}

    sortable_alias = namespace['TUserSortableCol']
    assert get_origin(sortable_alias) is Literal

    user_table_cls = namespace['UserTable']
    column_specs = user_table_cls.column_specs
    column_names = tuple(spec.name for spec in column_specs)
    assert set(get_args(sortable_alias)) == set(column_names)
    expected_ds = data_source_config(provider='sqlite', url='sqlite:///analytics.db', name=None)
    assert user_table_cls.datasource == expected_ds
    insert_payload = user_table_cls.serialize_insert({
        "id": None,
        "name": "A",
        "email": "a@example.com",
        "last_login": datetime.now(),
        "status": namespace["UserStatus"].ACTIVE,
    })
    assert insert_payload["status"] == namespace["UserStatus"].ACTIVE.value

    ds_mapping = generated_client.datasources
    assert ds_mapping == {
        'sqlite': data_source_config(provider='sqlite', url='sqlite:///analytics.db', name=None)
    }
    init_hints = get_type_hints(generated_client.__init__, globalns=namespace, localns=namespace)
    assert set(init_hints.keys()) == {"return", "echo_sql"}
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
    assert set(get_args(insert_data_type)) == {user_insert_cls, user_insert_dict}
    assert insert_hints['return'] is namespace['User']

    insert_many_hints = get_type_hints(user_table_cls.insert_many, globalns=namespace, localns=namespace)
    insert_many_data = insert_many_hints['data']
    assert get_origin(insert_many_data) is ABCSequence
    inner_union = get_args(insert_many_data)[0]
    assert set(get_args(inner_union)) == {user_insert_cls, user_insert_dict}
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

    find_first_hints = get_type_hints(user_table_cls.find_first, globalns=namespace, localns=namespace)
    assert find_first_hints['return'] == namespace['User'] | type(None)
    include_union_first = find_first_hints['include']
    include_args_first = set(get_args(include_union_first))
    assert type(None) in include_args_first
    include_args_first.remove(type(None))
    (include_dict_first,) = tuple(include_args_first)
    assert include_dict_first is namespace['UserIncludeDict']


def test_generated_client_supports_named_datasources() -> None:
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

    namespace: dict[str, Any] = {}
    generated_client_cls = None
    try:
        module = generate_client([PrimaryUser, SecondaryUser])
        exec(module.code, namespace)

        generated_client = namespace["Client"]
        generated_client_cls = generated_client
        data_source_config = namespace["DataSourceConfig"]
        expected_mapping = {
            "primary": data_source_config(provider="sqlite", url=f"sqlite:///{primary_db.as_posix()}", name="primary"),
            "secondary": data_source_config(provider="sqlite", url=f"sqlite:///{secondary_db.as_posix()}", name="secondary"),
        }
        assert generated_client.datasources == expected_mapping

        primary_table_cls = namespace["PrimaryUserTable"]
        secondary_table_cls = namespace["SecondaryUserTable"]
        assert primary_table_cls.datasource == expected_mapping["primary"]
        assert secondary_table_cls.datasource == expected_mapping["secondary"]

        client = generated_client()
        assert isinstance(client.primary_user, primary_table_cls)
        assert isinstance(client.secondary_user, secondary_table_cls)
        assert isinstance(client.primary_user._backend, SQLiteBackend)
        assert isinstance(client.secondary_user._backend, SQLiteBackend)
    finally:
        sys.modules.pop(module_name_primary, None)
        sys.modules.pop(module_name_secondary, None)
        primary_db.unlink(missing_ok=True)
        secondary_db.unlink(missing_ok=True)
        if generated_client_cls is not None:
            generated_client_cls.close_all()


def test_generated_client_written_module_allows_direct_import(tmp_path: Path) -> None:
    module = generate_client([User])
    target = resolve_generated_path()
    model_target, _ = compute_model_target(Path(__file__))
    model_init = model_target.parent / "__init__.py"
    backup = target.read_text(encoding="utf-8") if target.exists() else None
    try:
        target.write_text(module.code, encoding="utf-8")
        for mod in (
            "dclassql.client",
            "dclassql.generated_models",
            f"dclassql.generated_models.{model_target.stem}",
        ):
            sys.modules.pop(mod, None)
        import dclassql as dql
        importlib.reload(dql)
        Client = dql.Client

        assert Client.__name__ == "Client"
    finally:
        if backup is None:
            target.unlink(missing_ok=True)
        else:
            target.write_text(backup, encoding="utf-8")
        if model_target.exists() or model_target.is_symlink():
            model_target.unlink()
        if model_init.exists() and not any(
            p.name != "__init__.py" for p in model_init.parent.glob("*.py")
        ):
            model_init.unlink(missing_ok=True)
        for mod in (
            "dclassql.client",
            "dclassql.generated_models",
            f"dclassql.generated_models.{model_target.stem}",
        ):
            sys.modules.pop(mod, None)
        import dclassql as dql  # type: ignore[import]
        importlib.reload(dql)
