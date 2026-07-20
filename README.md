# DataclassQL

DataclassQL 是一个基于 **平凡 dataclass** 的 ORM 生成器, 可生成类型提示完整精巧的数据库客户端. 

模型文件保持干净、直观, 无需起手加一堆导入, 也没有 `mapped_column()`、`Annotation` 或额外的基类继承, 大部分时候只需要 `@dataclass`

---

## 设计目标

* **静态类型友好**: 全程可获得完美的补全体验。本库作为 [prisma client python](https://prisma-client-py.readthedocs.io/en/stable/) 的精神继承者, 致力于完成如下体验: 

![prisma client python 示例](https://prisma-client-py.readthedocs.io/en/stable/showcase.gif)

* **最小语法负担**: 模型定义是平凡的 Python dataclass, Python 本身就是 DSL
* **约定大于配置**: 常用定义只需写少量代码
* **零成本抽象**: 工具生成的数据库转换代码与手写版本有相同的速度

---

## 示例

```python
from dataclasses import dataclass
from datetime import datetime

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
        yield self.name, self.email
```

写出如下代码时: 

```python
from dclassql import Client

client = Client()

client.user.insert({
    "name": "Alice",
    "email": "test@example.com",
    # 这里缺少 last_login 
})
```

将在类型空间得到报错: 

```
error: Argument of type "dict[str, str]" cannot be assigned to parameter "data" of type "UserInsertDict" in function "insert"
    "last_login" is required in "UserInsertDict" (reportArgumentType)
```


## 安装

```
uv add dclassql
```

## 当前状态

DataclassQL 仍在早期开发阶段, 但不是无根浮萍, 我已经在另外两个项目里大量使用, 目前基于其他项目的反馈来更新. 

## 一份更长的例子

```python
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

```

生成的代码请见: https://github.com/myuanz/dataclassql/blob/master/tests/results.py
