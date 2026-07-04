---
name: dataclassql
description: 当用户要在 Python 项目中使用 dclassql/dataclassql 库时使用，包括编写普通 dataclass 模型、配置 __datasource__、生成类型安全 client、执行 push-db、调用 CRUD、定义关系/索引/主键，以及处理 JSON 值对象和常见生成问题。
---

# dataclassql

dataclassql 项目致力于做一个类似 Prisma for Python 那样的 ORM 工具(Prisma 的精神继任者), 工具使用前要根据模型定义生成类型安全的客户端代码, 这样项目里可获得在Python本身无法做到的类型标注, 同时生成静态的序列化反序列化代码, 极大缩减反射带来的性能损耗. 

## 适用场景

用户在某个项目里要使用 `dclassql/dataclassql`

## 工作原理

dataclassql 无需一大堆冗长的导入来表达类型、表、关系，约定大于配置，只需要原生的 `from dataclasses import dataclass`，写好模型文件后，执行

```bash
dclassql -m path/to/model.py generate
```

将会生成对应的类型安全的客户端，之后用户就可以调用客户端直接插入类型安全的 dict (通过 TypedDict) 或者 dataclass instance，查找时的 where 和返回结果同样是类型安全的. 

库已经发布到 pypi，包名是 `dclassql`

## 最小模型文件


```python
# model.py
from dataclasses import dataclass
from datetime import datetime

__datasource__ = {
    "url": "sqlite:///app.db",
}

@dataclass
class User:
    id: int
    name: str
    email: str | None
    created_at: datetime
```

生成 client：

```bash
dclassql -m path/to/model.py generate
```

默认会在模型文件同目录生成包：

```text
model_client/
  __init__.py
  __init__.pyi
  asdict.pyi
  client.py
```

如果模型文件叫 `base.py`，包名是 `base_client`，client 类名是 `BaseClient`.

## 基本使用

```python
from path.to.model_client import ModelClient

client = ModelClient()
client.push_db()

user = client.user.insert({
    "name": "Alice",
    "email": None,
    "created_at": datetime.now(),
})

rows = client.user.find_many(where={"name": "Alice"})
client.close()
```

`push_db(force_rebuild=False)` 默认不会执行不兼容重建.确认允许重建时：

```python
client.push_db(force_rebuild=True)
```

## 生成和推送命令

常用命令：

```bash
dclassql -m path/to/model.py generate
dclassql -m path/to/model.py push-db
```

生成的客户端有两种去向，一个是与 model.py 放在一起，一种是类 prisma 放到 dclassql 下的，后者可提供类似 `from dclassql import ...` 的风格使用. 此时只需: 

```bash
dclassql -m path/to/model.py generate --target package
```

大多数业务项目优先用默认的 `model-dir`，即生成在模型同目录.

push-db 可以预先在命令行执行，也可以在 runtime 执行 client.push_db(). 

## 主键和索引

将主键在`def primary_key(self):`函数内返回，例如

```python
@dataclass
class Position:
    account_id: str
    symbol: str

    def primary_key(self):
        return self.account_id, self.symbol # 主键是复合列
```

索引：

```python
def index(self):
    yield self.symbol # symbol 是索引
    yield self.account_id, self.symbol # (account_id, symbol) 也是索引

def unique_index(self):
    yield self.symbol # symbol 要唯一，同样可以后续跟 yield 增加更多
```

额外情况: 
- 没写 `primary_key()` 时，默认主键名是 `id`.
- `id: int` 默认作为 SQLite 自增主键，插入时可以传 `None` 或省略.
- 如果模型没有 `id`，应显式写 `primary_key()`，或者确认它不是表模型并放进 `__exclude__`.

## 关系和外键

关系字段引用另一个被收集的 dataclass 模型：

```python
@dataclass
class Address:
    id: int
    user_id: int
    location: str
    user: "User"

    def foreign_key(self):
        yield self.user.id == self.user_id, User.addresses # 读作「本模型的 `user 对象的 id` 记录在 `本模型的 user_id 字段`，本模型也可通过 `User.addresses` 访问」

@dataclass
class User:
    id: int
    name: str
    addresses: list[Address]
```

查询时可用 include：

```python
users = client.user.find_many(include={"addresses": True})
```

外键主要用于运行时关系解析，不创建数据库层面的真实外键约束. 1:1/1:n 都可支持

## 辅助 dataclass 和 JSON 字段

模块内所有 dataclass 默认都会被当作模型收集.辅助值对象不想建表时，用 `__exclude__`：

```python
@dataclass
class Stamp:
    dt: datetime
    idx: int

@dataclass
class Order:
    id: int
    stamp: Stamp

__exclude__ = (Stamp,)
```

被排除的 dataclass 字段会作为 JSON 值对象写入普通 `TEXT` 列，读取时还原为原 dataclass.也可以写字符串：

```python
__exclude__ = ("Stamp",)
```

判断规则：

- 被收集为模型的 dataclass 字段：关系.
- 未收集为模型的 dataclass 字段：JSON 值列.

## 类型别名和默认值

可以用 Python 3.12+ type alias：

```python
from typing import Literal
import math

type OrderSide = Literal["long", "short"]

@dataclass
class Order:
    id: int
    side: OrderSide
    price: float = math.nan
```

默认值可以来自导入对象，生成代码会尽量保留原模型默认值语义.

## 生成包里的文件

- `client.py`：真正的生成客户端、表访问类、TypedDict、Insert dataclass.
- `__init__.py`：运行时导出 client 类和 `asdict`.
- `__init__.pyi`：类型检查入口，让 `from xxx_client import asdict` 有精确类型.
- `asdict.pyi`：当前模型专用的 `asdict` 类型重载.

通常业务代码只需要：

```python
from .model_client import ModelClient, asdict
```
