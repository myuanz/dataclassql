from dataclasses import dataclass
from datetime import datetime
from typed_db.table_spec import TableInfo, Col
from typed_db.unwarp import unwarp

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///memory.db",
}

@dataclass
class User:
    id: int | None
    name: str
    email: str
    last_login: datetime

    def index(self):
        yield self.name
        yield self.last_login

    def unique_index(self):
        return self.name, self.email

@dataclass
class Book:
    id: int | None
    name: str

    def index(self):
        return self.name

@dataclass
class UserBook:
    user_id: int
    book_id: int
    created_at: datetime

    def primary_key(self):
        return (self.user_id, self.book_id)

    def index(self):
        yield self.created_at


def test_table_info():
    user = User(id=1, name="Alice", email="alice@example.com", last_login=datetime.now())
    book = Book(id=1, name="1984")
    user_book = UserBook(user_id=unwarp(user.id), book_id=unwarp(book.id), created_at=datetime.now())

    info = TableInfo.from_dc(User)
    assert info.primary_key.cols == Col('id', table=User)
    assert [idx.cols for idx in info.index] == [
        Col('name', table=User),
        Col('last_login', table=User),
        (Col('name', table=User), Col('email', table=User)),
    ]
    assert [idx.cols for idx in info.unique_index] == [
        (Col('name', table=User), Col('email', table=User)),
    ]

    info = TableInfo.from_dc(Book)
    assert info.primary_key.cols == Col('id', table=Book)

    info = TableInfo.from_dc(UserBook)
    assert info.primary_key.cols == (Col('user_id', table=UserBook), Col('book_id', table=UserBook))
    assert user_book.primary_key() == (user.id, book.id)
    assert next(user_book.index()) == user_book.created_at


@dataclass
class A:
    id: int | None
    name: str
    email: str

    def index(self):
        return self.name, self.email

def test_primary_key_with_return():
    info = TableInfo.from_dc(A)
    assert info.primary_key.cols == Col('id', table=A)
    assert [idx.cols for idx in info.index] == [
        (Col('name', table=A), Col('email', table=A)),
    ]
