from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from test_codegen import Address, BirthDay, Book, User, UserBook
from typing import Any, Literal, Mapping, Sequence, TypedDict

@dataclass(slots=True)
class DataSourceConfig:
    provider: str
    url: str | None


@dataclass(slots=True)
class ForeignKeySpec:
    local_columns: tuple[str, ...]
    remote_model: type[Any]
    remote_columns: tuple[str, ...]
    backref: str | None


TAddressIncludeCol = Literal['User']
TAddressSortableCol = Literal['id', 'location', 'user_id']

@dataclass(slots=True)
class AddressInsert:
    id: int | None
    location: str
    user_id: int

class AddressInsertDict(TypedDict):
    id: int | None
    location: str
    user_id: int

class AddressWhereDict(TypedDict, total=False):
    id: int | None
    location: str | None
    user_id: int | None

class AddressTable:
    model = Address
    insert_model = AddressInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db')
    columns = ('id', 'location', 'user_id')
    primary_key = ('id',)
    indexes = ()
    unique_indexes = ()
    foreign_keys = (
        ForeignKeySpec(
            local_columns=('user_id',),
            remote_model=User,
            remote_columns=('id',),
            backref='addresses',
        ),
    )

    def __init__(self, backend: Any) -> None:
        self._backend = backend

    def insert(self, data: AddressInsert | AddressInsertDict) -> Address:
        raise NotImplementedError('Database insert is not implemented yet')

    def insert_many(self, data: Sequence[AddressInsert | AddressInsertDict]) -> list[Address]:
        raise NotImplementedError('Database insert_many is not implemented yet')

    def find_many(self, *, where: AddressWhereDict | None = None, include: dict[TAddressIncludeCol, bool] | None = None, order_by: Sequence[tuple[TAddressSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[Address]:
        raise NotImplementedError('Query generation is not implemented yet')

    def find_first(self, *, where: AddressWhereDict | None = None, include: dict[TAddressIncludeCol, bool] | None = None, order_by: Sequence[tuple[TAddressSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> Address | None:
        raise NotImplementedError('Query generation is not implemented yet')

TBirthDayIncludeCol = Literal['User']
TBirthDaySortableCol = Literal['user_id', 'date']

@dataclass(slots=True)
class BirthDayInsert:
    user_id: int
    date: datetime

class BirthDayInsertDict(TypedDict):
    user_id: int
    date: datetime

class BirthDayWhereDict(TypedDict, total=False):
    user_id: int | None
    date: datetime | None

class BirthDayTable:
    model = BirthDay
    insert_model = BirthDayInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db')
    columns = ('user_id', 'date')
    primary_key = ('user_id',)
    indexes = ()
    unique_indexes = ()
    foreign_keys = (
        ForeignKeySpec(
            local_columns=('user_id',),
            remote_model=User,
            remote_columns=('id',),
            backref='birthday',
        ),
    )

    def __init__(self, backend: Any) -> None:
        self._backend = backend

    def insert(self, data: BirthDayInsert | BirthDayInsertDict) -> BirthDay:
        raise NotImplementedError('Database insert is not implemented yet')

    def insert_many(self, data: Sequence[BirthDayInsert | BirthDayInsertDict]) -> list[BirthDay]:
        raise NotImplementedError('Database insert_many is not implemented yet')

    def find_many(self, *, where: BirthDayWhereDict | None = None, include: dict[TBirthDayIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBirthDaySortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[BirthDay]:
        raise NotImplementedError('Query generation is not implemented yet')

    def find_first(self, *, where: BirthDayWhereDict | None = None, include: dict[TBirthDayIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBirthDaySortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> BirthDay | None:
        raise NotImplementedError('Query generation is not implemented yet')

TBookIncludeCol = Literal['UserBook']
TBookSortableCol = Literal['id', 'name']

@dataclass(slots=True)
class BookInsert:
    id: int | None
    name: str

class BookInsertDict(TypedDict):
    id: int | None
    name: str

class BookWhereDict(TypedDict, total=False):
    id: int | None
    name: str | None

class BookTable:
    model = Book
    insert_model = BookInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db')
    columns = ('id', 'name')
    primary_key = ('id',)
    indexes = (('name',),)
    unique_indexes = ()
    foreign_keys = ()

    def __init__(self, backend: Any) -> None:
        self._backend = backend

    def insert(self, data: BookInsert | BookInsertDict) -> Book:
        raise NotImplementedError('Database insert is not implemented yet')

    def insert_many(self, data: Sequence[BookInsert | BookInsertDict]) -> list[Book]:
        raise NotImplementedError('Database insert_many is not implemented yet')

    def find_many(self, *, where: BookWhereDict | None = None, include: dict[TBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBookSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[Book]:
        raise NotImplementedError('Query generation is not implemented yet')

    def find_first(self, *, where: BookWhereDict | None = None, include: dict[TBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBookSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> Book | None:
        raise NotImplementedError('Query generation is not implemented yet')

TUserIncludeCol = Literal['Address', 'BirthDay', 'UserBook']
TUserSortableCol = Literal['id', 'name', 'email', 'last_login']

@dataclass(slots=True)
class UserInsert:
    id: int | None
    name: str
    email: str
    last_login: datetime

class UserInsertDict(TypedDict):
    id: int | None
    name: str
    email: str
    last_login: datetime

class UserWhereDict(TypedDict, total=False):
    id: int | None
    name: str | None
    email: str | None
    last_login: datetime | None

class UserTable:
    model = User
    insert_model = UserInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db')
    columns = ('id', 'name', 'email', 'last_login')
    primary_key = ('id',)
    indexes = (('name',), ('name', 'email'), ('last_login',),)
    unique_indexes = ()
    foreign_keys = ()

    def __init__(self, backend: Any) -> None:
        self._backend = backend

    def insert(self, data: UserInsert | UserInsertDict) -> User:
        raise NotImplementedError('Database insert is not implemented yet')

    def insert_many(self, data: Sequence[UserInsert | UserInsertDict]) -> list[User]:
        raise NotImplementedError('Database insert_many is not implemented yet')

    def find_many(self, *, where: UserWhereDict | None = None, include: dict[TUserIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[User]:
        raise NotImplementedError('Query generation is not implemented yet')

    def find_first(self, *, where: UserWhereDict | None = None, include: dict[TUserIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> User | None:
        raise NotImplementedError('Query generation is not implemented yet')

TUserBookIncludeCol = Literal['Book', 'User']
TUserBookSortableCol = Literal['user_id', 'book_id', 'created_at']

@dataclass(slots=True)
class UserBookInsert:
    user_id: int
    book_id: int
    created_at: datetime

class UserBookInsertDict(TypedDict):
    user_id: int
    book_id: int
    created_at: datetime

class UserBookWhereDict(TypedDict, total=False):
    user_id: int | None
    book_id: int | None
    created_at: datetime | None

class UserBookTable:
    model = UserBook
    insert_model = UserBookInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db')
    columns = ('user_id', 'book_id', 'created_at')
    primary_key = ('user_id', 'book_id')
    indexes = (('created_at',),)
    unique_indexes = ()
    foreign_keys = (
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

    def __init__(self, backend: Any) -> None:
        self._backend = backend

    def insert(self, data: UserBookInsert | UserBookInsertDict) -> UserBook:
        raise NotImplementedError('Database insert is not implemented yet')

    def insert_many(self, data: Sequence[UserBookInsert | UserBookInsertDict]) -> list[UserBook]:
        raise NotImplementedError('Database insert_many is not implemented yet')

    def find_many(self, *, where: UserBookWhereDict | None = None, include: dict[TUserBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserBookSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[UserBook]:
        raise NotImplementedError('Query generation is not implemented yet')

    def find_first(self, *, where: UserBookWhereDict | None = None, include: dict[TUserBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserBookSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> UserBook | None:
        raise NotImplementedError('Query generation is not implemented yet')

class GeneratedClient:
    datasources = {
        'sqlite': DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db'),
    }

    def __init__(self, connections: Mapping[str, Any]) -> None:
        self._connections = connections
        for key in self.datasources.keys():
            if key not in connections:
                raise KeyError(f'datasource {key} missing connection')
        self.address = AddressTable(connections['sqlite'])
        self.birth_day = BirthDayTable(connections['sqlite'])
        self.book = BookTable(connections['sqlite'])
        self.user = UserTable(connections['sqlite'])
        self.user_book = UserBookTable(connections['sqlite'])

__all__ = ("DataSourceConfig", "ForeignKeySpec", "GeneratedClient", "TAddressIncludeCol", "TAddressSortableCol", "AddressInsert", "AddressInsertDict", "AddressWhereDict", "AddressTable", "TBirthDayIncludeCol", "TBirthDaySortableCol", "BirthDayInsert", "BirthDayInsertDict", "BirthDayWhereDict", "BirthDayTable", "TBookIncludeCol", "TBookSortableCol", "BookInsert", "BookInsertDict", "BookWhereDict", "BookTable", "TUserIncludeCol", "TUserSortableCol", "UserInsert", "UserInsertDict", "UserWhereDict", "UserTable", "TUserBookIncludeCol", "TUserBookSortableCol", "UserBookInsert", "UserBookInsertDict", "UserBookWhereDict", "UserBookTable",)
