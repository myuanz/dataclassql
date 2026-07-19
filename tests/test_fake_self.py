from dataclasses import dataclass
from datetime import datetime
import pytest
from dclassql.model_inspector.table_constraints import Col, ColGroup, TableConstraints
from dclassql.unwarp import unwarp

__datasource__ = {
    "provider": "sqlite",
    "url": "sqlite:///memory.db",
}

@dataclass
class User:
    id: int
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
    id: int
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


def test_table_constraints():
    user = User(id=1, name="Alice", email="alice@example.com", last_login=datetime.now())
    book = Book(id=1, name="1984")
    user_book = UserBook(user_id=unwarp(user.id), book_id=unwarp(book.id), created_at=datetime.now())

    info = TableConstraints.from_dc(User)
    assert info.primary_key == ColGroup((Col('id'),))
    assert [group.cols for group in info.indexes] == [
        (Col('name'),),
        (Col('last_login'),),
    ]
    assert [group.cols for group in info.unique_indexes] == [
        (Col('name'), Col('email')),
    ]

    info = TableConstraints.from_dc(Book)
    assert info.primary_key == ColGroup((Col('id'),))

    info = TableConstraints.from_dc(UserBook)
    assert info.primary_key.cols == (Col('user_id'), Col('book_id'))
    assert user_book.primary_key() == (user.id, book.id)
    assert next(user_book.index()) == user_book.created_at


@dataclass
class A:
    id: int
    name: str
    email: str

    def index(self):
        return self.name, self.email

def test_primary_key_with_return():
    info = TableConstraints.from_dc(A)
    assert info.primary_key == ColGroup((Col('id'),))
    assert [group.cols for group in info.indexes] == [
        (Col('name'), Col('email')),
    ]


@dataclass
class GeneratorPK:
    left_id: int
    right_id: int

    def primary_key(self):
        yield (self.left_id, self.right_id)


def test_primary_key_generator_tuple_error():
    with pytest.raises(TypeError, match=r'May be you meant to use "return" instead?'):
        TableConstraints.from_dc(GeneratorPK)
