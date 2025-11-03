# TypedDB

TypedDB is a Python library that provides typed database access, originating from code snippets I've been copying and pasting across various projects for a long time. It will expand based on my needs in the future.


## Installation

```bash
uv add typed-db[sqlite]
```

> Only sqlite is supported now, more db may be added in the future.

## Usage

```python
from dataclasses import dataclass
from typing import reveal_type
from fastlite import database
from typed_db import TypedTable


@dataclass
class User:
    id: int | None
    name: str
    email: str

    def index(self):
        yield self.name
        yield self.email


db = database(':memory:')
user_tb = TypedTable[User, db] # table will be created if not exists, and indexes will be created too
reveal_type(user_tb) # -> TypedTable[User]

user = User(id=None, name='Alice', email='alice@example.com')
user = user_tb.insert(user)  # insert and get the inserted user with id

users = user_tb()
print(users) # [User(id=1, name='Alice', email='alice@example.com')]

user = user_tb.where_one(name = 'Alice')
reveal_type(user) # User | None
print(user) # User(id=1, name='Alice', email='alice@example.com')

user = user_tb.where_one(name = 'Bob')
print(user) # None

print(user_tb.q(f'select name from {user_tb} order by name limit 1', as_list=True)) # [{'name': 'Alice'}]
print(db.schema)

'''
CREATE TABLE [user] (
   [id] INTEGER PRIMARY KEY,
   [name] TEXT,
   [email] TEXT
);
CREATE INDEX idx_user_name ON "user" (name);
CREATE INDEX idx_user_email ON "user" (email);
'''

```
