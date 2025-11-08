from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Literal, Mapping, Sequence, NotRequired
from typing_extensions import TypedDict

from dclassql import DataSourceConfig
from dclassql.db_pool import BaseDBPool, save_local
from dclassql.runtime.backends import BackendProtocol, ColumnSpec, ForeignKeySpec, RelationSpec
from dclassql.runtime.backends.protocols import TableProtocol
from dclassql.runtime.datasource import open_sqlite_connection

from datetime import datetime
from tests.test_codegen import Address, BirthDay, Book, User, UserBook, UserStatus, UserType, UserVIPLevel

class DateTimeFilter(TypedDict, total=False, closed=True):
    EQ: datetime | None
    IN: Sequence[datetime]
    NOT_IN: Sequence[datetime]
    LT: datetime
    LTE: datetime
    GT: datetime
    GTE: datetime
    NOT: DateTimeFilter | datetime | None


class IntFilter(TypedDict, total=False, closed=True):
    EQ: int | None
    IN: Sequence[int]
    NOT_IN: Sequence[int]
    LT: int
    LTE: int
    GT: int
    GTE: int
    NOT: IntFilter | int | None


class StringFilter(TypedDict, total=False, closed=True):
    EQ: str | None
    IN: Sequence[str]
    NOT_IN: Sequence[str]
    LT: str
    LTE: str
    GT: str
    GTE: str
    CONTAINS: str
    STARTS_WITH: str
    ENDS_WITH: str
    NOT: StringFilter | str | None




TAddressIncludeCol = Literal['user']
TAddressSortableCol = Literal['id', 'location', 'user_id']

@dataclass(slots=True, kw_only=True)
class AddressInsert:
    id: int | None = None
    location: str
    user_id: int


class AddressDict(TypedDict, closed=True):
    id: int
    location: str
    user_id: int
    user: UserDict | None


class AddressInsertDict(TypedDict, closed=True):
    id: NotRequired[int]
    location: str
    user_id: int


class AddressUserRelationFilter(TypedDict, total=False, closed=True):
    IS: UserWhereDict | None
    IS_NOT: UserWhereDict | None



class AddressWhereDict(TypedDict, total=False, closed=True):
    id: int | None | IntFilter
    location: str | None | StringFilter
    user_id: int | None | IntFilter
    user: AddressUserRelationFilter
    AND: AddressWhereDict | Sequence[AddressWhereDict]
    OR: Sequence[AddressWhereDict]
    NOT: AddressWhereDict | Sequence[AddressWhereDict]


class AddressIncludeDict(TypedDict, total=False, closed=True):
    user: bool

class AddressOrderByDict(TypedDict, total=False, closed=True):
    id: Literal['asc', 'desc']
    location: Literal['asc', 'desc']
    user_id: Literal['asc', 'desc']

class AddressTable(TableProtocol):
    model = Address
    insert_model = AddressInsert
    table_name: str = 'Address'
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='id', optional=False, auto_increment=True, has_default=False, has_default_factory=False),
        ColumnSpec(name='location', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='user_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    primary_key: tuple[str, ...] = ('id',)
    indexes: tuple[tuple[str, ...], ...] = ()
    unique_indexes: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[ForeignKeySpec, ...] = (
        ForeignKeySpec(
            local_columns=('user_id',),
            remote_model=User,
            remote_columns=('id',),
            backref='addresses',
        ),
    )

    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='user', table_name='UserTable', table_module=__name__, many=False, mapping=(('user_id', 'id'),), table_factory=lambda: UserTable),
    )


    @classmethod
    def serialize_insert(cls, data: AddressInsert | Mapping[str, object]) -> dict[str, object]:
        if isinstance(data, Mapping):
            result: dict[str, object] = {}
            if 'id' in data:
                result['id'] = data['id']
            if 'location' in data:
                result['location'] = data['location']
            if 'user_id' in data:
                result['user_id'] = data['user_id']
            return result
        if isinstance(data, AddressInsert):
            return {
                'id': data.id,
                'location': data.location,
                'user_id': data.user_id,
            }
        raise TypeError("Unsupported insert payload type for Address")

    @classmethod
    def deserialize_row(cls, row: Mapping[str, object]) -> Address:
        instance = Address.__new__(Address)  # type: ignore[call-arg]
        instance.id = row['id'] # type: ignore[attr-defined]
        instance.location = row['location'] # type: ignore[attr-defined]
        instance.user_id = row['user_id'] # type: ignore[attr-defined]
        instance.user = None # type: ignore[attr-defined]
        return instance

    def __init__(self, backend: BackendProtocol) -> None:
        self._backend = backend

    def __str__(self) -> str:
        return self._backend.escape_identifier(self.table_name)

    def insert(self, data: AddressInsert | AddressInsertDict) -> Address:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[AddressInsert | AddressInsertDict], *, batch_size: int | None = None) -> list[Address]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: AddressWhereDict | None = None, include: AddressIncludeDict | None = None, order_by: AddressOrderByDict | None = None, take: int | None = None, skip: int | None = None) -> list[Address]:
        return self._backend.find_many(
            self, 
            where=where, include=include, order_by=order_by, 
            take=take, skip=skip
        )

    def find_first(self, *, where: AddressWhereDict | None = None, include: AddressIncludeDict | None = None, order_by: AddressOrderByDict | None = None, skip: int | None = None) -> Address | None:
        return self._backend.find_first(
            self, 
            where=where, include=include, order_by=order_by, 
            skip=skip
        )
TBirthDayIncludeCol = Literal['user']
TBirthDaySortableCol = Literal['user_id', 'date']

@dataclass(slots=True, kw_only=True)
class BirthDayInsert:
    user_id: int
    date: datetime


class BirthDayDict(TypedDict, closed=True):
    user_id: int
    user: UserDict | None
    date: datetime


class BirthDayInsertDict(TypedDict, closed=True):
    user_id: int
    date: datetime


class BirthDayUserRelationFilter(TypedDict, total=False, closed=True):
    IS: UserWhereDict | None
    IS_NOT: UserWhereDict | None



class BirthDayWhereDict(TypedDict, total=False, closed=True):
    user_id: int | None | IntFilter
    date: datetime | None | DateTimeFilter
    user: BirthDayUserRelationFilter
    AND: BirthDayWhereDict | Sequence[BirthDayWhereDict]
    OR: Sequence[BirthDayWhereDict]
    NOT: BirthDayWhereDict | Sequence[BirthDayWhereDict]


class BirthDayIncludeDict(TypedDict, total=False, closed=True):
    user: bool

class BirthDayOrderByDict(TypedDict, total=False, closed=True):
    user_id: Literal['asc', 'desc']
    date: Literal['asc', 'desc']

class BirthDayTable(TableProtocol):
    model = BirthDay
    insert_model = BirthDayInsert
    table_name: str = 'BirthDay'
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='user_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='date', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    primary_key: tuple[str, ...] = ('user_id',)
    indexes: tuple[tuple[str, ...], ...] = ()
    unique_indexes: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[ForeignKeySpec, ...] = (
        ForeignKeySpec(
            local_columns=('user_id',),
            remote_model=User,
            remote_columns=('id',),
            backref='birthday',
        ),
    )

    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='user', table_name='UserTable', table_module=__name__, many=False, mapping=(('user_id', 'id'),), table_factory=lambda: UserTable),
    )


    @classmethod
    def serialize_insert(cls, data: BirthDayInsert | Mapping[str, object]) -> dict[str, object]:
        if isinstance(data, Mapping):
            result: dict[str, object] = {}
            if 'user_id' in data:
                result['user_id'] = data['user_id']
            if 'date' in data:
                result['date'] = data['date']
            return result
        if isinstance(data, BirthDayInsert):
            return {
                'user_id': data.user_id,
                'date': data.date,
            }
        raise TypeError("Unsupported insert payload type for BirthDay")

    @classmethod
    def deserialize_row(cls, row: Mapping[str, object]) -> BirthDay:
        instance = BirthDay.__new__(BirthDay)  # type: ignore[call-arg]
        instance.user_id = row['user_id'] # type: ignore[attr-defined]
        instance.user = None # type: ignore[attr-defined]
        instance.date = row['date'] # type: ignore[attr-defined]
        return instance

    def __init__(self, backend: BackendProtocol) -> None:
        self._backend = backend

    def __str__(self) -> str:
        return self._backend.escape_identifier(self.table_name)

    def insert(self, data: BirthDayInsert | BirthDayInsertDict) -> BirthDay:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[BirthDayInsert | BirthDayInsertDict], *, batch_size: int | None = None) -> list[BirthDay]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: BirthDayWhereDict | None = None, include: BirthDayIncludeDict | None = None, order_by: BirthDayOrderByDict | None = None, take: int | None = None, skip: int | None = None) -> list[BirthDay]:
        return self._backend.find_many(
            self, 
            where=where, include=include, order_by=order_by, 
            take=take, skip=skip
        )

    def find_first(self, *, where: BirthDayWhereDict | None = None, include: BirthDayIncludeDict | None = None, order_by: BirthDayOrderByDict | None = None, skip: int | None = None) -> BirthDay | None:
        return self._backend.find_first(
            self, 
            where=where, include=include, order_by=order_by, 
            skip=skip
        )
TBookIncludeCol = Literal['users']
TBookSortableCol = Literal['id', 'name']

@dataclass(slots=True, kw_only=True)
class BookInsert:
    id: int | None = None
    name: str


class BookDict(TypedDict, closed=True):
    id: int
    name: str
    users: list[UserBookDict]


class BookInsertDict(TypedDict, closed=True):
    id: NotRequired[int]
    name: str


class BookUsersRelationFilter(TypedDict, total=False, closed=True):
    SOME: UserBookWhereDict | None
    NONE: UserBookWhereDict | None
    EVERY: UserBookWhereDict



class BookWhereDict(TypedDict, total=False, closed=True):
    id: int | None | IntFilter
    name: str | None | StringFilter
    users: BookUsersRelationFilter
    AND: BookWhereDict | Sequence[BookWhereDict]
    OR: Sequence[BookWhereDict]
    NOT: BookWhereDict | Sequence[BookWhereDict]


class BookIncludeDict(TypedDict, total=False, closed=True):
    users: bool

class BookOrderByDict(TypedDict, total=False, closed=True):
    id: Literal['asc', 'desc']
    name: Literal['asc', 'desc']

class BookTable(TableProtocol):
    model = Book
    insert_model = BookInsert
    table_name: str = 'Book'
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='id', optional=False, auto_increment=True, has_default=False, has_default_factory=False),
        ColumnSpec(name='name', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    primary_key: tuple[str, ...] = ('id',)
    indexes: tuple[tuple[str, ...], ...] = (('name',),)
    unique_indexes: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[ForeignKeySpec, ...] = ()

    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='users', table_name='UserBookTable', table_module=__name__, many=True, mapping=(('id', 'book_id'),), table_factory=lambda: UserBookTable),
    )


    @classmethod
    def serialize_insert(cls, data: BookInsert | Mapping[str, object]) -> dict[str, object]:
        if isinstance(data, Mapping):
            result: dict[str, object] = {}
            if 'id' in data:
                result['id'] = data['id']
            if 'name' in data:
                result['name'] = data['name']
            return result
        if isinstance(data, BookInsert):
            return {
                'id': data.id,
                'name': data.name,
            }
        raise TypeError("Unsupported insert payload type for Book")

    @classmethod
    def deserialize_row(cls, row: Mapping[str, object]) -> Book:
        instance = Book.__new__(Book)  # type: ignore[call-arg]
        instance.id = row['id'] # type: ignore[attr-defined]
        instance.name = row['name'] # type: ignore[attr-defined]
        instance.users = [] # type: ignore[attr-defined]
        return instance

    def __init__(self, backend: BackendProtocol) -> None:
        self._backend = backend

    def __str__(self) -> str:
        return self._backend.escape_identifier(self.table_name)

    def insert(self, data: BookInsert | BookInsertDict) -> Book:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[BookInsert | BookInsertDict], *, batch_size: int | None = None) -> list[Book]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: BookWhereDict | None = None, include: BookIncludeDict | None = None, order_by: BookOrderByDict | None = None, take: int | None = None, skip: int | None = None) -> list[Book]:
        return self._backend.find_many(
            self, 
            where=where, include=include, order_by=order_by, 
            take=take, skip=skip
        )

    def find_first(self, *, where: BookWhereDict | None = None, include: BookIncludeDict | None = None, order_by: BookOrderByDict | None = None, skip: int | None = None) -> Book | None:
        return self._backend.find_first(
            self, 
            where=where, include=include, order_by=order_by, 
            skip=skip
        )
TUserIncludeCol = Literal['addresses', 'birthday', 'books']
TUserSortableCol = Literal['id', 'name', 'email', 'last_login', 'status', 'type', 'vip_level']

@dataclass(slots=True, kw_only=True)
class UserInsert:
    id: int | None = None
    name: str
    email: str
    last_login: datetime
    status: UserStatus
    type: UserType
    vip_level: UserVIPLevel | None


class UserDict(TypedDict, closed=True):
    id: int
    name: str
    email: str
    last_login: datetime
    status: UserStatus
    type: UserType
    vip_level: UserVIPLevel | None
    birthday: BirthDayDict | None
    addresses: list[AddressDict]
    books: list[UserBookDict]


class UserInsertDict(TypedDict, closed=True):
    id: NotRequired[int]
    name: str
    email: str
    last_login: datetime
    status: UserStatus
    type: UserType
    vip_level: UserVIPLevel | None


class UserBirthdayRelationFilter(TypedDict, total=False, closed=True):
    IS: BirthDayWhereDict | None
    IS_NOT: BirthDayWhereDict | None


class UserAddressesRelationFilter(TypedDict, total=False, closed=True):
    SOME: AddressWhereDict | None
    NONE: AddressWhereDict | None
    EVERY: AddressWhereDict


class UserBooksRelationFilter(TypedDict, total=False, closed=True):
    SOME: UserBookWhereDict | None
    NONE: UserBookWhereDict | None
    EVERY: UserBookWhereDict



class UserWhereDict(TypedDict, total=False, closed=True):
    id: int | None | IntFilter
    name: str | None | StringFilter
    email: str | None | StringFilter
    last_login: datetime | None | DateTimeFilter
    status: UserStatus | None
    type: UserType | None | StringFilter
    vip_level: UserVIPLevel | None | IntFilter
    birthday: UserBirthdayRelationFilter
    addresses: UserAddressesRelationFilter
    books: UserBooksRelationFilter
    AND: UserWhereDict | Sequence[UserWhereDict]
    OR: Sequence[UserWhereDict]
    NOT: UserWhereDict | Sequence[UserWhereDict]


class UserIncludeDict(TypedDict, total=False, closed=True):
    addresses: bool
    birthday: bool
    books: bool

class UserOrderByDict(TypedDict, total=False, closed=True):
    id: Literal['asc', 'desc']
    name: Literal['asc', 'desc']
    email: Literal['asc', 'desc']
    last_login: Literal['asc', 'desc']
    status: Literal['asc', 'desc']
    type: Literal['asc', 'desc']
    vip_level: Literal['asc', 'desc']

class UserTable(TableProtocol):
    model = User
    insert_model = UserInsert
    table_name: str = 'User'
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='id', optional=False, auto_increment=True, has_default=False, has_default_factory=False),
        ColumnSpec(name='name', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='email', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='last_login', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='status', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='type', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='vip_level', optional=True, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    primary_key: tuple[str, ...] = ('id',)
    indexes: tuple[tuple[str, ...], ...] = (('name',), ('name', 'email'), ('last_login',),)
    unique_indexes: tuple[tuple[str, ...], ...] = (('name', 'email'),)
    foreign_keys: tuple[ForeignKeySpec, ...] = ()

    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='birthday', table_name='BirthDayTable', table_module=__name__, many=False, mapping=(('id', 'user_id'),), table_factory=lambda: BirthDayTable),
        RelationSpec(name='addresses', table_name='AddressTable', table_module=__name__, many=True, mapping=(('id', 'user_id'),), table_factory=lambda: AddressTable),
        RelationSpec(name='books', table_name='UserBookTable', table_module=__name__, many=True, mapping=(('id', 'user_id'),), table_factory=lambda: UserBookTable),
    )


    @classmethod
    def serialize_insert(cls, data: UserInsert | Mapping[str, object]) -> dict[str, object]:
        if isinstance(data, Mapping):
            result: dict[str, object] = {}
            if 'id' in data:
                result['id'] = data['id']
            if 'name' in data:
                result['name'] = data['name']
            if 'email' in data:
                result['email'] = data['email']
            if 'last_login' in data:
                result['last_login'] = data['last_login']
            if 'status' in data:
                result['status'] = data['status'].value  # type: ignore[attr-defined]
            if 'type' in data:
                result['type'] = data['type'].value  # type: ignore[attr-defined]
            if 'vip_level' in data:
                result['vip_level'] = (data['vip_level'].value if data['vip_level'] is not None else None)  # type: ignore[attr-defined]
            return result
        if isinstance(data, UserInsert):
            return {
                'id': data.id,
                'name': data.name,
                'email': data.email,
                'last_login': data.last_login,
                'status': data.status.value,  # type: ignore[attr-defined]
                'type': data.type.value,  # type: ignore[attr-defined]
                'vip_level': (data.vip_level.value if data.vip_level is not None else None),  # type: ignore[attr-defined]
            }
        raise TypeError("Unsupported insert payload type for User")

    @classmethod
    def deserialize_row(cls, row: Mapping[str, object]) -> User:
        instance = User.__new__(User)  # type: ignore[call-arg]
        instance.id = row['id'] # type: ignore[attr-defined]
        instance.name = row['name'] # type: ignore[attr-defined]
        instance.email = row['email'] # type: ignore[attr-defined]
        instance.last_login = row['last_login'] # type: ignore[attr-defined]
        instance.status = UserStatus(row['status']) # type: ignore[attr-defined]
        instance.type = UserType(row['type']) # type: ignore[attr-defined]
        instance.vip_level = (UserVIPLevel(row['vip_level']) if row['vip_level'] is not None else None) # type: ignore[attr-defined]
        instance.birthday = None # type: ignore[attr-defined]
        instance.addresses = [] # type: ignore[attr-defined]
        instance.books = [] # type: ignore[attr-defined]
        return instance

    def __init__(self, backend: BackendProtocol) -> None:
        self._backend = backend

    def __str__(self) -> str:
        return self._backend.escape_identifier(self.table_name)

    def insert(self, data: UserInsert | UserInsertDict) -> User:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[UserInsert | UserInsertDict], *, batch_size: int | None = None) -> list[User]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: UserWhereDict | None = None, include: UserIncludeDict | None = None, order_by: UserOrderByDict | None = None, take: int | None = None, skip: int | None = None) -> list[User]:
        return self._backend.find_many(
            self, 
            where=where, include=include, order_by=order_by, 
            take=take, skip=skip
        )

    def find_first(self, *, where: UserWhereDict | None = None, include: UserIncludeDict | None = None, order_by: UserOrderByDict | None = None, skip: int | None = None) -> User | None:
        return self._backend.find_first(
            self, 
            where=where, include=include, order_by=order_by, 
            skip=skip
        )
TUserBookIncludeCol = Literal['book', 'user']
TUserBookSortableCol = Literal['user_id', 'book_id', 'created_at']

@dataclass(slots=True, kw_only=True)
class UserBookInsert:
    user_id: int
    book_id: int
    created_at: datetime


class UserBookDict(TypedDict, closed=True):
    user_id: int
    book_id: int
    user: UserDict | None
    book: BookDict | None
    created_at: datetime


class UserBookInsertDict(TypedDict, closed=True):
    user_id: int
    book_id: int
    created_at: datetime


class UserBookUserRelationFilter(TypedDict, total=False, closed=True):
    IS: UserWhereDict | None
    IS_NOT: UserWhereDict | None


class UserBookBookRelationFilter(TypedDict, total=False, closed=True):
    IS: BookWhereDict | None
    IS_NOT: BookWhereDict | None



class UserBookWhereDict(TypedDict, total=False, closed=True):
    user_id: int | None | IntFilter
    book_id: int | None | IntFilter
    created_at: datetime | None | DateTimeFilter
    user: UserBookUserRelationFilter
    book: UserBookBookRelationFilter
    AND: UserBookWhereDict | Sequence[UserBookWhereDict]
    OR: Sequence[UserBookWhereDict]
    NOT: UserBookWhereDict | Sequence[UserBookWhereDict]


class UserBookIncludeDict(TypedDict, total=False, closed=True):
    book: bool
    user: bool

class UserBookOrderByDict(TypedDict, total=False, closed=True):
    user_id: Literal['asc', 'desc']
    book_id: Literal['asc', 'desc']
    created_at: Literal['asc', 'desc']

class UserBookTable(TableProtocol):
    model = UserBook
    insert_model = UserBookInsert
    table_name: str = 'UserBook'
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='user_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='book_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='created_at', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    primary_key: tuple[str, ...] = ('user_id', 'book_id')
    indexes: tuple[tuple[str, ...], ...] = (('created_at',),)
    unique_indexes: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[ForeignKeySpec, ...] = (
        ForeignKeySpec(
            local_columns=('user_id',),
            remote_model=User,
            remote_columns=('id',),
            backref='books',
        ),
        ForeignKeySpec(
            local_columns=('book_id',),
            remote_model=Book,
            remote_columns=('id',),
            backref='users',
        ),
    )

    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='user', table_name='UserTable', table_module=__name__, many=False, mapping=(('user_id', 'id'),), table_factory=lambda: UserTable),
        RelationSpec(name='book', table_name='BookTable', table_module=__name__, many=False, mapping=(('book_id', 'id'),), table_factory=lambda: BookTable),
    )


    @classmethod
    def serialize_insert(cls, data: UserBookInsert | Mapping[str, object]) -> dict[str, object]:
        if isinstance(data, Mapping):
            result: dict[str, object] = {}
            if 'user_id' in data:
                result['user_id'] = data['user_id']
            if 'book_id' in data:
                result['book_id'] = data['book_id']
            if 'created_at' in data:
                result['created_at'] = data['created_at']
            return result
        if isinstance(data, UserBookInsert):
            return {
                'user_id': data.user_id,
                'book_id': data.book_id,
                'created_at': data.created_at,
            }
        raise TypeError("Unsupported insert payload type for UserBook")

    @classmethod
    def deserialize_row(cls, row: Mapping[str, object]) -> UserBook:
        instance = UserBook.__new__(UserBook)  # type: ignore[call-arg]
        instance.user_id = row['user_id'] # type: ignore[attr-defined]
        instance.book_id = row['book_id'] # type: ignore[attr-defined]
        instance.user = None # type: ignore[attr-defined]
        instance.book = None # type: ignore[attr-defined]
        instance.created_at = row['created_at'] # type: ignore[attr-defined]
        return instance

    def __init__(self, backend: BackendProtocol) -> None:
        self._backend = backend

    def __str__(self) -> str:
        return self._backend.escape_identifier(self.table_name)

    def insert(self, data: UserBookInsert | UserBookInsertDict) -> UserBook:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[UserBookInsert | UserBookInsertDict], *, batch_size: int | None = None) -> list[UserBook]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: UserBookWhereDict | None = None, include: UserBookIncludeDict | None = None, order_by: UserBookOrderByDict | None = None, take: int | None = None, skip: int | None = None) -> list[UserBook]:
        return self._backend.find_many(
            self, 
            where=where, include=include, order_by=order_by, 
            take=take, skip=skip
        )

    def find_first(self, *, where: UserBookWhereDict | None = None, include: UserBookIncludeDict | None = None, order_by: UserBookOrderByDict | None = None, skip: int | None = None) -> UserBook | None:
        return self._backend.find_first(
            self, 
            where=where, include=include, order_by=order_by, 
            skip=skip
        )
class Client(BaseDBPool):
    _echo_sql: bool = False
    datasources = {
        'sqlite': DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None),
    }

    @classmethod
    @save_local
    def _backend_sqlite(cls, *, echo_sql: bool | None = None) -> BackendProtocol:
        config = cls.datasources['sqlite']
        backend_echo = cls._echo_sql if echo_sql is None else echo_sql
        if config.provider == 'sqlite':
            from dclassql.runtime.backends.sqlite import SQLiteBackend
            conn = open_sqlite_connection(config.url)
            cls._setup_sqlite_db(conn)
            return SQLiteBackend(conn, echo_sql=backend_echo)
        raise ValueError(f"Unsupported provider '{config.provider}' for datasource 'sqlite'")

    def __init__(self, *, echo_sql: bool = False) -> None:
        self._echo_sql = echo_sql
        self.address = AddressTable(self._backend_sqlite(echo_sql=echo_sql))
        self.birth_day = BirthDayTable(self._backend_sqlite(echo_sql=echo_sql))
        self.book = BookTable(self._backend_sqlite(echo_sql=echo_sql))
        self.user = UserTable(self._backend_sqlite(echo_sql=echo_sql))
        self.user_book = UserBookTable(self._backend_sqlite(echo_sql=echo_sql))

    @classmethod
    def close_all(cls, verbose: bool = False) -> None:
        super().close_all(verbose=verbose)
        if hasattr(cls._local, '_backend_sqlite'):
            backend = getattr(cls._local, '_backend_sqlite')
            if hasattr(backend, 'close') and callable(getattr(backend, 'close')):
                backend.close()
            delattr(cls._local, '_backend_sqlite')

__all__ = (
    "DataSourceConfig",
    "ForeignKeySpec",
    "Client",
    "TAddressIncludeCol",
    "TAddressSortableCol",
    "AddressIncludeDict",
    "AddressOrderByDict",
    "AddressDict",
    "AddressInsert",
    "AddressInsertDict",
    "AddressWhereDict",
    "AddressTable",
    "AddressUserRelationFilter",
    "TBirthDayIncludeCol",
    "TBirthDaySortableCol",
    "BirthDayIncludeDict",
    "BirthDayOrderByDict",
    "BirthDayDict",
    "BirthDayInsert",
    "BirthDayInsertDict",
    "BirthDayWhereDict",
    "BirthDayTable",
    "BirthDayUserRelationFilter",
    "TBookIncludeCol",
    "TBookSortableCol",
    "BookIncludeDict",
    "BookOrderByDict",
    "BookDict",
    "BookInsert",
    "BookInsertDict",
    "BookWhereDict",
    "BookTable",
    "BookUsersRelationFilter",
    "TUserIncludeCol",
    "TUserSortableCol",
    "UserIncludeDict",
    "UserOrderByDict",
    "UserDict",
    "UserInsert",
    "UserInsertDict",
    "UserWhereDict",
    "UserTable",
    "UserBirthdayRelationFilter",
    "UserAddressesRelationFilter",
    "UserBooksRelationFilter",
    "TUserBookIncludeCol",
    "TUserBookSortableCol",
    "UserBookIncludeDict",
    "UserBookOrderByDict",
    "UserBookDict",
    "UserBookInsert",
    "UserBookInsertDict",
    "UserBookWhereDict",
    "UserBookTable",
    "UserBookUserRelationFilter",
    "UserBookBookRelationFilter",
)
