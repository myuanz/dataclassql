# 项目总体目标

本项目致力于做一个类似 Prisma for Python 那样的 ORM 工具(Prisma 的精神继任者), 工具使用前要根据模型定义生成类型安全的客户端代码, 这样项目里可获得在Python本身无法做到的类型标注, 同时生成静态的序列化反序列化代码, 极大缩减反射带来的性能损耗. 

另外, 本项目希望模型定义足够干净, 就像写普通的 dataclass 一样, 而不是起手`from xxx import Col, Field, BaseModel, relationship`, 然后给每个字段写一个`xxx: Annotation[int, yyy] = mapped_column(zzz=t)`. 保证生成模型用的代码在 pyright/mypy 看起来是类型安全复合直觉的

例如可以声明以下模型:

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
        yield self.name                  # 第一个索引
        yield self.last_login, self.name # 第二个索引, 且是复合索引
        yield self.last_login            # 第三个索引


@dataclass
class Address:
    id: int
    location: str

    user_id: int
    user: User

    def foreign_key(self): 
        yield self.user.id == self.user_id, User.addresses # 本表的 user 的 id 就是本表的 user_id, 并与 User 那边叫做 addresses

```
来生成如下代码: 
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


def _User_serializer(data: UserInsert | Mapping[str, object]) -> dict[str, object]:
    if isinstance(data, UserInsert):
        data = asdict(data)

    payload = {
        'id': data['id'] if 'id' in data else None, # 只对自增主键有此待遇
        'name': data['name'],
        'email': data['email'],
        'last_login': data['last_login'],
        'status': data['status'].value,
        'vip_level': data['vip_level'].value,
    }
    return payload


def _User_deserializer(row: Mapping[str, object]) -> User:
    instance = User(
        id=row['id'],
        name=row['name'],
        email=row['email'],
        last_login=row['last_login'],
        status=UserStatus(row['status']),
        vip_level=UserVipLevel(row['vip_level']),
    )
    return instance

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
        include: UserIncludeDict | None = None,
        order_by: UserOrderByDict | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[User]: ...
    def find_first(
        self,
        *,
        where: UserWhereDict | None = None,
        include: UserIncludeDict | None = None,
        order_by: UserOrderByDict | None = None,
        skip: int | None = None,
    ) -> User | None: ...
```

为何要走二部生成路线? 在插入User时理论上只需要 name、last_login 就足够, 而插入后则 id 一定存在, 如果我们只是把 id 标注为 int | None, 那么插入后或者查询后的 User 就需要额外对 id 判空, 如果标注为 int, 则插入前必须要指定一个 id. 如果要在 dataclass 之外解决这个问题, 那就要像其他 orm 一样列一堆堆的导入. 


# 路线图

- [x] 元编程工具集, 收集 dataclass 字段信息等
- [x] 基本的类型化代码生成 (Insert/InsertDict + insert/insert_many/find_many/find_first)
- [x] sqlite 后端:
    - [x] 对应 prisma db push 的功能, 如: 创建表, 创建索引, 变更表结构和变更索引等
    - [x] 运行时查询和插入等功能
- [x] 命令行接口包括: `typed-db -m {model py file} push-db`、`typed-db -m {model py file} generate`, model py file 默认名为`model.py`
- [x] 惰性 n+1 查询关联表
- [x] 使用 jinja 生成代码
- [x] rich filter, 类似 Prisma 的过滤器, 可以写 find_first(where={'or': {'a': {'eq': 1}, 'other_relation': {'is': {'name': '2'}}}})
  - [x] 标量过滤器
  - [x] 逻辑组合
  - [ ] json过滤器
  - [x] 关系过滤器
- [x] echo sql 模式
- [ ] 多个数据源文件的客户端
- [x] 静态化数据库序列化和反序列化
- [x] 枚举字段读写转换, 支持 Python Enum 与数据库值的往返映射

# 设计

- 完全在 Python 中定义表, 就像写 dataclass 一样, 没有奇怪的 col 或者 field. 通过`def foreign_key(self): yield xxx`, `def index(self): yield xxx`, `def primary_key(self): yield xxx`来标注, 使用见下. 生成表时, 通过传入一个虚假的 self 获取对应的列, 见 @src/dclassql/table_spec.py 中的 TableInfo/FakeSelf/KeySpec/Col. 
- 虚拟外键, 外键只是查询意义上的东西, 不在数据库生成
- 与 Prisma 类似的 n+1 机制, 可以在find系列函数里设置include=, 也可以不include, 但在获取对象时即时查询
- 使用 fake self 机制获取主键、索引、外键、唯键等信息
- 不依赖 fastlite, 只依赖 sqlite-utils
- 初期仅支持 `insert` / `insert_many` / `find_many` / `find_first` 的代码生成. Insert 支持 dataclass 与 TypedDict 两种结构, WhereDict 会独立生成并把所有列标注为可选
- 每个模型模块通过模块级 `__datasource__ = {"provider": ..., "url": ...}` 指定数据源(目前仅支持 sqlite), 代码生成时会按 provider 分组构建表与客户端, 生成的 `Client` 在初始化时需要提供 `{provider: connection}` 的映射
- 生成结果包含 `DataSourceConfig`、`ForeignKeySpec` 等元信息, 以及 `T{Name}IncludeCol`/`T{Name}SortableCol` 字面量类型别名、`*Insert` dataclass、`*InsertDict`、`*WhereDict`、`*IncludeDict` 与 `*OrderByDict` TypedDict 组合、具体的 `*Table` 表访问类以及聚合的 `Client`
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

生成的客户端中, 还会包含 `Client` 类, 构造时把所有表的 `*Table` 实例挂到蛇形命名的属性上, 方便业务方直接调用。
