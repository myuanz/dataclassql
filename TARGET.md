# 项目总体目标

本项目致力于做一个类似 Prisma for Python 那样的 ORM 工具, 工具使用前要根据模型定义生成类型安全的客户端代码, 这样项目里可获得在Python本身无法做到的类型标注. 

例如对于模型:

```
@dataclass
class User:
    id: int
    name: str
    email: str | None
    last_login: datetime
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    addresses: list[Address]

    def index(self):
        yield self.name
        yield self.last_login, self.name # 复合索引
        yield self.last_login


@dataclass
class Address:
    id: int
    location: str

    user_id: int
    user: User

    def foreign_key(self): 
        yield self.user.id == self.user_id, User.addresses

```

在插入时理论上只需要一个 name、last_login 就足够插入, 而插入后则 id 一定存在, 如果我们只是把 id 标注为 int | None, 那么插入后或者查询后的 User 就需要额外对 id 判空, 如果标注为 int, 则插入前必须要指定一个 id. 

因此要分成两步, 基于这样的 User 生成:
```
TUserIncludeCol = Literal['Address', 'BirthDay', 'UserBook']
TUserSortableCol = Literal['id', 'name', 'email', 'last_login', 'created_at']

@dataclass(slots=True)
class UserInsert:
    id: int | None
    name: str
    email: str | None
    last_login: datetime
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

class UserInsertDict(TypedDict):
    id: int | None
    name: str
    email: str | None
    last_login: datetime
    created_at: datetime

class UserWhereDict(TypedDict, total=False):
    id: int | None
    name: str | None
    email: str | None
    last_login: datetime | None
    created_at: datetime | None


class UserTable:
    model = User
    insert_model = UserInsert
    columns = ("id", "name", "email", "last_login", "created_at")
    primary_key = ("id",)

    def insert(self, data: UserInsert | UserInsertDict) -> User: ...
    def insert_many(self, data: Sequence[UserInsert | UserInsertDict]) -> list[User]: ...
    def find_many(
        self,
        *,
        where: UserWhereDict | None = None,
        include: dict[TUserIncludeCol, bool] | None = None,
        order_by: Sequence[tuple[TUserSortableCol, Literal['asc', 'desc']]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[User]: ...
    def find_first(
        self,
        *,
        where: UserWhereDict | None = None,
        include: dict[TUserIncludeCol, bool] | None = None,
        order_by: Sequence[tuple[TUserSortableCol, Literal['asc', 'desc']]] | None = None,
        skip: int | None = None,
    ) -> User | None: ...
```


# 路线图

- [x] 元编程工具集, 收集 dataclass 字段信息等
- [x] 基本的类型化代码生成 (Insert/InsertDict + insert/insert_many/find_many/find_first)
- [ ] sqlite 后端:
    - [ ] 对应 prisma db push 的功能, 如: 创建表, 创建索引, 变更表结构和变更索引等
    - [ ] 运行时查询和插入等功能

# 设计

- 完全在 Python 中定义表, 就像写 dataclass 一样, 没有奇怪的 col 或者 field. 通过`def foreign_key(self): yield xxx`, `def index(self): yield xxx`, `def primary_key(self): yield xxx`来标注, 使用见下. 生成表时, 通过传入一个虚假的 self 获取对应的列, 见 @src/typed_db/table_spec.py 中的 TableInfo/FakeSelf/KeySpec/Col. 
- 虚拟外键, 外键只是查询意义上的东西, 不在数据库生成
- 与 Prisma 类似的 n+1 机制, 可以在find系列函数里设置include=, 也可以不include, 但在获取对象时即时查询
- 使用 fake self 机制获取主键、索引、外键、唯键等信息
- 不依赖 fastlite, 只依赖 sqlite-utils
- 初期仅支持 `insert` / `insert_many` / `find_many` / `find_first` 的代码生成. Insert 支持 dataclass 与 TypedDict 两种结构, WhereDict 会独立生成并把所有列标注为可选
- 每个模型模块通过模块级 `__datasource__ = {"provider": ..., "url": ...}` 指定数据源(目前仅支持 sqlite), 代码生成时会按 provider 分组构建表与客户端, `GeneratedClient` 在初始化时需要提供 `{provider: connection}` 的映射
- 生成结果包含 `DataSourceConfig`、`ForeignKeySpec` 等元信息, 以及 `T{Name}IncludeCol`/`T{Name}SortableCol` 字面量类型别名、`*Insert` dataclass、`*InsertDict` 与 `*WhereDict` TypedDict 组合、具体的 `*Table` 表访问类以及聚合的 `GeneratedClient`
- 每个 model 文件里需要写明数据源, 不同的 model 可以有不同的数据源. 在后面调用时按数据源分组使用. model 文件下可以定义模块变量 __datasource__ = {'provider': xxx, 'url': xxx}, 像 Prisma 一样, 提供器是数据库, url是连接url
    - 未来会支持其他数据库, 现在只关注 sqlite
    - 未来会支持从环境变量, 现在先不管
- 相关sql使用`pypika`生成

## 期待的样例

```python
from typing import get_args

@dataclass
class Address:
    id: int # 如果一个表有一个叫 id 的 int 字段, 那么默认这就是自增的主键. 如果id有自增属性, 那么为插入生成的结构定义id是可空的, 如果没有自增, 则此字段不可空
    location: str

    user_id: int
    user: 'User' # 这里 User 还没定义, 用引号包一下

    def foreign_key(self): 
        yield self.user.id == self.user_id, User.addresses # 意为: user_id 这个列对应着 User.id, 当用户调用 address.user 时, 如果已经在查询时 include User, 则直接给出结果, 否则生成一个对 User 的查询. 另外, 在 User 那一端, User.addresses 代表本表

@dataclass
class BirthDay:
    user_id: int
    user: 'User' # 这里 User 还没定义, 用引号包一下

    date: datetime

    def primary_key(self):
        return self.user_id # 此处指定 pk 是 user_id
    
    def foreign_key(self):
        yield self.user.id == self.user_id, User.birthday # 同前, user_id对应user.id, 并且User.birthday将对应本表

@dataclass
class User:
    id: int
    name: str
    email: str
    last_login: datetime

    birthday: BirthDay | None # 生日可能没有设置过

    addresses: list[Address] # 调用 user.addresses 时, 如果已经在查询时 include Address, 则直接给结果, 否则生成一个对 list[Address] 的查询, 查询时具体怎么选择键, 在Address的foreign_key已经定义

    books: list[UserBook]

    def index(self):
        yield self.name
        yield self.name, self.email # 复合索引
        yield self.last_login

    def unique(self):
        yield self.name, self.email # 复合索引并且加 unique

@dataclass
class Book:
    id: int
    name: str

    users: list[UserBook]
    def index(self):
        return self.name

@dataclass
class UserBook:
    user_id: int
    book_id: int

    user: User
    book: Book

    created_at: datetime

    def primary_key(self):
        return (self.user_id, self.book_id)

    def index(self):
        yield self.created_at

    def foreign_key(self):
        yield self.user.id == self.user_id, User.books # 这两个含义跟 address 相同
        yield self.book.id == self.book_id, Book.users

```

生成的客户端中, 还会包含 `GeneratedClient` 类, 构造时把所有表的 `*Table` 实例挂到蛇形命名的属性上, 方便业务方直接调用。
