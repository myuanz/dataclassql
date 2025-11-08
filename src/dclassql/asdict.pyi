from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, overload

from .client import (
    Address,
    AddressDict,
    BirthDay,
    BirthDayDict,
    Book,
    BookDict,
    User,
    UserDict,
    UserBook,
    UserBookDict,
)

RelationPolicy = Literal['skip', 'fetch', 'keep']

@overload
def asdict(value: Address, *, relation_policy: RelationPolicy = 'keep') -> AddressDict: ...

@overload
def asdict(value: BirthDay, *, relation_policy: RelationPolicy = 'keep') -> BirthDayDict: ...

@overload
def asdict(value: Book, *, relation_policy: RelationPolicy = 'keep') -> BookDict: ...

@overload
def asdict(value: User, *, relation_policy: RelationPolicy = 'keep') -> UserDict: ...

@overload
def asdict(value: UserBook, *, relation_policy: RelationPolicy = 'keep') -> UserBookDict: ...

@overload
def asdict(value: Sequence[Address], *, relation_policy: RelationPolicy = 'keep') -> list[AddressDict]: ...

@overload
def asdict(value: Sequence[BirthDay], *, relation_policy: RelationPolicy = 'keep') -> list[BirthDayDict]: ...

@overload
def asdict(value: Sequence[Book], *, relation_policy: RelationPolicy = 'keep') -> list[BookDict]: ...

@overload
def asdict(value: Sequence[User], *, relation_policy: RelationPolicy = 'keep') -> list[UserDict]: ...

@overload
def asdict(value: Sequence[UserBook], *, relation_policy: RelationPolicy = 'keep') -> list[UserBookDict]: ...

@overload
def asdict(value: Sequence[Any], *, relation_policy: RelationPolicy = 'keep') -> list[Any]: ...

@overload
def asdict(value: None, *, relation_policy: RelationPolicy = 'keep') -> None: ...

def asdict(value: object, *, relation_policy: RelationPolicy = 'keep') -> Any: ...
