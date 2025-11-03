from __future__ import annotations

from collections.abc import Sequence as ABCSequence
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Literal, Mapping, get_args, get_origin, get_type_hints

from typed_db.codegen import generate_client

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///analytics.db",
}

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
    id: int | None
    name: str
    email: str
    last_login: datetime
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
    generated_client = namespace['GeneratedClient']

    include_alias = namespace['TUserIncludeCol']
    assert get_origin(include_alias) is Literal
    assert set(get_args(include_alias)) == {'Address', 'BirthDay', 'UserBook'}

    sortable_alias = namespace['TUserSortableCol']
    assert get_origin(sortable_alias) is Literal

    user_table_cls = namespace['UserTable']
    columns_tuple = user_table_cls.columns
    assert set(get_args(sortable_alias)) == set(columns_tuple)
    assert user_table_cls.datasource == data_source_config(provider='sqlite', url='sqlite:///analytics.db')

    ds_mapping = generated_client.datasources
    assert ds_mapping == {
        'sqlite': data_source_config(provider='sqlite', url='sqlite:///analytics.db')
    }
    init_hints = get_type_hints(generated_client.__init__, globalns=namespace, localns=namespace)
    assert init_hints['connections'] == Mapping[str, Any]

    user_insert_cls = namespace['UserInsert']
    user_insert_dict = namespace['UserInsertDict']
    user_where_dict = namespace['UserWhereDict']

    insert_field_names = [f.name for f in fields(user_insert_cls)]
    assert insert_field_names == list(columns_tuple)

    user_model_hints = get_type_hints(User)
    insert_hints = get_type_hints(user_insert_cls, globalns=namespace, localns=namespace)
    assert set(get_args(insert_hints['id'])) == {int, type(None)}
    assert insert_hints['email'] == user_model_hints['email']
    assert insert_hints['last_login'] == user_model_hints['last_login']

    insert_dict_hints = get_type_hints(user_insert_dict, globalns=namespace, localns=namespace)
    assert set(get_args(insert_dict_hints['id'])) == {int, type(None)}
    assert insert_dict_hints['email'] == user_model_hints['email']
    assert insert_dict_hints['last_login'] == user_model_hints['last_login']
    assert insert_dict_hints == insert_hints

    assert getattr(user_insert_dict, '__total__') is True
    assert getattr(user_where_dict, '__total__') is False
    assert user_insert_dict not in user_where_dict.__mro__

    where_hints = get_type_hints(user_where_dict, globalns=namespace, localns=namespace)
    for name, hint in where_hints.items():
        assert name in insert_dict_hints
        got_args = set(get_args(hint)) or {hint}
        assert type(None) in got_args
        got_args.remove(type(None))
        expected_hint = insert_dict_hints[name]
        expected_args = set(get_args(expected_hint)) or {expected_hint}
        expected_args.discard(type(None))
        assert got_args == expected_args

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
    assert get_origin(include_dict_type) is dict
    dict_key_type, dict_value_type = get_args(include_dict_type)
    assert dict_key_type is include_alias
    assert dict_value_type is bool

    order_union = find_many_hints['order_by']
    order_args = set(get_args(order_union))
    assert type(None) in order_args
    order_args.remove(type(None))
    (order_seq_type,) = tuple(order_args)
    assert get_origin(order_seq_type) is ABCSequence
    order_tuple_type = get_args(order_seq_type)[0]
    assert get_origin(order_tuple_type) is tuple
    literal_type = get_args(order_tuple_type)[1]
    assert get_origin(literal_type) is Literal
    assert set(get_args(literal_type)) == {'asc', 'desc'}

    find_first_hints = get_type_hints(user_table_cls.find_first, globalns=namespace, localns=namespace)
    assert find_first_hints['return'] == namespace['User'] | type(None)
    include_union_first = find_first_hints['include']
    include_args_first = set(get_args(include_union_first))
    assert type(None) in include_args_first
    include_args_first.remove(type(None))
    (include_dict_first,) = tuple(include_args_first)
    assert get_origin(include_dict_first) is dict
    assert get_args(include_dict_first)[0] is include_alias
