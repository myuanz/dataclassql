from __future__ import annotations

from dataclasses import dataclass, field
from dclassql.db_pool import BaseDBPool, save_local
from dclassql.runtime.backends import BackendProtocol, ColumnSpec, ForeignKeySpec, RelationSpec, create_backend
from dclassql.runtime.datasource import open_sqlite_connection
from types import MappingProxyType
import sqlite3
from datetime import datetime
from test_codegen import Address, BirthDay, Book, User, UserBook
from typing import Any, Literal, Mapping, NotRequired, Sequence, TypedDict, cast

@dataclass(slots=True)
class DataSourceConfig:
    provider: str
    url: str | None
    name: str | None = None

    @property
    def key(self) -> str:
        return self.name or self.provider




TAddressIncludeCol = Literal['User']
TAddressSortableCol = Literal['id', 'location', 'user_id']

@dataclass(slots=True, kw_only=True)
class AddressInsert:
    id: int | None = None
    location: str
    user_id: int

class AddressInsertDict(TypedDict):
    id: NotRequired[int]
    location: str
    user_id: int

class AddressWhereDict(TypedDict, total=False):
    id: int | None
    location: str | None
    user_id: int | None

class AddressTable:
    model = Address
    insert_model = AddressInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    columns: tuple[str, ...] = ('id', 'location', 'user_id')
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='id', optional=False, auto_increment=True, has_default=False, has_default_factory=False),
        ColumnSpec(name='location', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='user_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    auto_increment_columns: tuple[str, ...] = ('id',)
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

    def __init__(self, backend: BackendProtocol[Address, AddressInsert, AddressWhereDict]) -> None:
        self._backend: BackendProtocol[Address, AddressInsert, AddressWhereDict] = backend

    def insert(self, data: AddressInsert | AddressInsertDict) -> Address:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[AddressInsert | AddressInsertDict], *, batch_size: int | None = None) -> list[Address]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: AddressWhereDict | None = None, include: dict[TAddressIncludeCol, bool] | None = None, order_by: Sequence[tuple[TAddressSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[Address]:
        return self._backend.find_many(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, take=take, skip=skip)

    def find_first(self, *, where: AddressWhereDict | None = None, include: dict[TAddressIncludeCol, bool] | None = None, order_by: Sequence[tuple[TAddressSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> Address | None:
        return self._backend.find_first(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, skip=skip)

TBirthDayIncludeCol = Literal['User']
TBirthDaySortableCol = Literal['user_id', 'date']

@dataclass(slots=True, kw_only=True)
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
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    columns: tuple[str, ...] = ('user_id', 'date')
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='user_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='date', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    auto_increment_columns: tuple[str, ...] = ()
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

    def __init__(self, backend: BackendProtocol[BirthDay, BirthDayInsert, BirthDayWhereDict]) -> None:
        self._backend: BackendProtocol[BirthDay, BirthDayInsert, BirthDayWhereDict] = backend

    def insert(self, data: BirthDayInsert | BirthDayInsertDict) -> BirthDay:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[BirthDayInsert | BirthDayInsertDict], *, batch_size: int | None = None) -> list[BirthDay]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: BirthDayWhereDict | None = None, include: dict[TBirthDayIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBirthDaySortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[BirthDay]:
        return self._backend.find_many(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, take=take, skip=skip)

    def find_first(self, *, where: BirthDayWhereDict | None = None, include: dict[TBirthDayIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBirthDaySortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> BirthDay | None:
        return self._backend.find_first(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, skip=skip)

TBookIncludeCol = Literal['UserBook']
TBookSortableCol = Literal['id', 'name']

@dataclass(slots=True, kw_only=True)
class BookInsert:
    id: int | None = None
    name: str

class BookInsertDict(TypedDict):
    id: NotRequired[int]
    name: str

class BookWhereDict(TypedDict, total=False):
    id: int | None
    name: str | None

class BookTable:
    model = Book
    insert_model = BookInsert
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    columns: tuple[str, ...] = ('id', 'name')
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='id', optional=False, auto_increment=True, has_default=False, has_default_factory=False),
        ColumnSpec(name='name', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    auto_increment_columns: tuple[str, ...] = ('id',)
    primary_key: tuple[str, ...] = ('id',)
    indexes: tuple[tuple[str, ...], ...] = (('name',),)
    unique_indexes: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[ForeignKeySpec, ...] = ()
    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='users', table_name='UserBookTable', table_module=__name__, many=True, mapping=(('id', 'book_id'),), table_factory=lambda: UserBookTable),
    )

    def __init__(self, backend: BackendProtocol[Book, BookInsert, BookWhereDict]) -> None:
        self._backend: BackendProtocol[Book, BookInsert, BookWhereDict] = backend

    def insert(self, data: BookInsert | BookInsertDict) -> Book:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[BookInsert | BookInsertDict], *, batch_size: int | None = None) -> list[Book]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: BookWhereDict | None = None, include: dict[TBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBookSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[Book]:
        return self._backend.find_many(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, take=take, skip=skip)

    def find_first(self, *, where: BookWhereDict | None = None, include: dict[TBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TBookSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> Book | None:
        return self._backend.find_first(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, skip=skip)

TUserIncludeCol = Literal['Address', 'BirthDay', 'UserBook']
TUserSortableCol = Literal['id', 'name', 'email', 'last_login']

@dataclass(slots=True, kw_only=True)
class UserInsert:
    id: int | None = None
    name: str
    email: str
    last_login: datetime

class UserInsertDict(TypedDict):
    id: NotRequired[int]
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
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    columns: tuple[str, ...] = ('id', 'name', 'email', 'last_login')
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='id', optional=False, auto_increment=True, has_default=False, has_default_factory=False),
        ColumnSpec(name='name', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='email', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='last_login', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    auto_increment_columns: tuple[str, ...] = ('id',)
    primary_key: tuple[str, ...] = ('id',)
    indexes: tuple[tuple[str, ...], ...] = (('name',), ('name', 'email'), ('last_login',),)
    unique_indexes: tuple[tuple[str, ...], ...] = (('name', 'email'),)
    foreign_keys: tuple[ForeignKeySpec, ...] = ()
    relations: tuple[RelationSpec, ...] = (
        RelationSpec(name='birthday', table_name='BirthDayTable', table_module=__name__, many=False, mapping=(('id', 'user_id'),), table_factory=lambda: BirthDayTable),
        RelationSpec(name='addresses', table_name='AddressTable', table_module=__name__, many=True, mapping=(('id', 'user_id'),), table_factory=lambda: AddressTable),
        RelationSpec(name='books', table_name='UserBookTable', table_module=__name__, many=True, mapping=(('id', 'user_id'),), table_factory=lambda: UserBookTable),
    )

    def __init__(self, backend: BackendProtocol[User, UserInsert, UserWhereDict]) -> None:
        self._backend: BackendProtocol[User, UserInsert, UserWhereDict] = backend

    def insert(self, data: UserInsert | UserInsertDict) -> User:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[UserInsert | UserInsertDict], *, batch_size: int | None = None) -> list[User]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: UserWhereDict | None = None, include: dict[TUserIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[User]:
        return self._backend.find_many(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, take=take, skip=skip)

    def find_first(self, *, where: UserWhereDict | None = None, include: dict[TUserIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> User | None:
        return self._backend.find_first(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, skip=skip)

TUserBookIncludeCol = Literal['Book', 'User']
TUserBookSortableCol = Literal['user_id', 'book_id', 'created_at']

@dataclass(slots=True, kw_only=True)
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
    datasource = DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None)
    columns: tuple[str, ...] = ('user_id', 'book_id', 'created_at')
    column_specs: tuple[ColumnSpec, ...] = (
        ColumnSpec(name='user_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='book_id', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
        ColumnSpec(name='created_at', optional=False, auto_increment=False, has_default=False, has_default_factory=False),
    )
    column_specs_by_name: Mapping[str, ColumnSpec] = MappingProxyType({spec.name: spec for spec in column_specs})
    auto_increment_columns: tuple[str, ...] = ()
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

    def __init__(self, backend: BackendProtocol[UserBook, UserBookInsert, UserBookWhereDict]) -> None:
        self._backend: BackendProtocol[UserBook, UserBookInsert, UserBookWhereDict] = backend

    def insert(self, data: UserBookInsert | UserBookInsertDict) -> UserBook:
        return self._backend.insert(self, data)

    def insert_many(self, data: Sequence[UserBookInsert | UserBookInsertDict], *, batch_size: int | None = None) -> list[UserBook]:
        return self._backend.insert_many(self, data, batch_size=batch_size)

    def find_many(self, *, where: UserBookWhereDict | None = None, include: dict[TUserBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserBookSortableCol, Literal['asc', 'desc']]] | None = None, take: int | None = None, skip: int | None = None) -> list[UserBook]:
        return self._backend.find_many(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, take=take, skip=skip)

    def find_first(self, *, where: UserBookWhereDict | None = None, include: dict[TUserBookIncludeCol, bool] | None = None, order_by: Sequence[tuple[TUserBookSortableCol, Literal['asc', 'desc']]] | None = None, skip: int | None = None) -> UserBook | None:
        return self._backend.find_first(self, where=where, include=cast(Mapping[str, bool] | None, include), order_by=order_by, skip=skip)

class Client(BaseDBPool):
    datasources = {
        'sqlite': DataSourceConfig(provider='sqlite', url='sqlite:///analytics.db', name=None),
    }

    @classmethod
    @save_local
    def _backend_sqlite(cls) -> BackendProtocol[Any, Any, Mapping[str, object]]:
        config = cls.datasources['sqlite']
        if config.provider == 'sqlite':
            conn = open_sqlite_connection(config.url)
            cls._setup_sqlite_db(conn)
            return create_backend('sqlite', conn)
        raise ValueError(f"Unsupported provider '{config.provider}' for datasource 'sqlite'")

    def __init__(self) -> None:
        self.address = AddressTable(cast(BackendProtocol[Address, AddressInsert, AddressWhereDict], self._backend_sqlite()))
        self.birth_day = BirthDayTable(cast(BackendProtocol[BirthDay, BirthDayInsert, BirthDayWhereDict], self._backend_sqlite()))
        self.book = BookTable(cast(BackendProtocol[Book, BookInsert, BookWhereDict], self._backend_sqlite()))
        self.user = UserTable(cast(BackendProtocol[User, UserInsert, UserWhereDict], self._backend_sqlite()))
        self.user_book = UserBookTable(cast(BackendProtocol[UserBook, UserBookInsert, UserBookWhereDict], self._backend_sqlite()))

    @classmethod
    def close_all(cls, verbose: bool = False) -> None:
        super().close_all(verbose=verbose)
        if hasattr(cls._local, '_backend_sqlite'):
            backend = getattr(cls._local, '_backend_sqlite')
            if hasattr(backend, 'close') and callable(getattr(backend, 'close')):
                backend.close()
            delattr(cls._local, '_backend_sqlite')

__all__ = ("DataSourceConfig", "ForeignKeySpec", "Client", "TAddressIncludeCol", "TAddressSortableCol", "AddressInsert", "AddressInsertDict", "AddressWhereDict", "AddressTable", "TBirthDayIncludeCol", "TBirthDaySortableCol", "BirthDayInsert", "BirthDayInsertDict", "BirthDayWhereDict", "BirthDayTable", "TBookIncludeCol", "TBookSortableCol", "BookInsert", "BookInsertDict", "BookWhereDict", "BookTable", "TUserIncludeCol", "TUserSortableCol", "UserInsert", "UserInsertDict", "UserWhereDict", "UserTable", "TUserBookIncludeCol", "TUserBookSortableCol", "UserBookInsert", "UserBookInsertDict", "UserBookWhereDict", "UserBookTable",)
