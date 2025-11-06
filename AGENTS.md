# AGENTS

## 项目概览
- **DataclassQL** 是一个围绕“纯 dataclass 定义”构建的 ORM 客户端生成器, 版本号当前为 `0.1.3`, 目标是成为 Prisma for Python 的精神继任者。
- 通过分析模型 dataclass, 自动生成带完整类型提示的客户端及表访问层, 以获得静态类型检查能力 (pyright/mypy) 与更直观的开发体验。
- 目前聚焦 SQLite, 已实现代码生成、数据库 schema 推送、运行时 CRUD 与包含机制, 并提供懒加载的关系解析。

## 技术栈与依赖
- 要求 Python ≥ 3.12。
- 核心依赖: `jinja2` (模板渲染)、`pypika` (SQL 构造与 schema 操作)、`typing_extensions` (Typing 支持)。
- 建议使用 `uv` 管理环境与指令, 例如: `uv add dclassql`, `uv run pytest .`, `uv run pyright .`。

## 目录速览
- `src/dclassql/codegen.py` + `templates/client_module.py.jinja`: 负责生成客户端模块代码。
- `src/dclassql/model_inspector.py` 与 `table_spec.py`: 将 dataclass 转换为列、索引、外键等结构描述。
- `src/dclassql/push/`: 数据库 schema 推送逻辑, 当前实现 `SQLitePusher`。
- `src/dclassql/runtime/`: 运行时后端、懒加载关系、数据源解析与 sqlite 适配器。
- `src/dclassql/cli.py`: `dql` 命令入口, 提供 `generate` / `push-db` 子命令。
- `src/dclassql/generated_models/`: 生成模型样例 (用于测试/示例)。
- `tests/`: 覆盖 CLI、代码生成、schema 推送、运行时、类型检查等场景。

## 代码生成流水线
- `generate_client(models)` 是入口, 先调用 `inspect_models` 收集 dataclass 结构信息 (`ModelInfo` / `ColumnInfo` 等)。
- `_TypeRenderer` 负责把 Python 类型对象转成字符串表示, 处理 `Annotated`、`UnionType` 等复杂类型。
- 模板渲染产物包括:
  - `Client` 类: 维护数据源配置、延迟初始化每个表的后端对象。
  - `*Table` 类: 封装 `insert` / `insert_many` / `find_many` / `find_first` 等方法, 依赖运行时后端。
  - `*Insert` dataclass、`*InsertDict` / `*WhereDict` / `*IncludeDict` / `*OrderByDict` TypedDict, 以及 `T*IncludeCol` / `T*SortableCol` Literal 类型别名。
  - `ForeignKeySpec`、`ColumnSpec`、`RelationSpec` 等元信息用于运行时懒加载。
- 生成代码写入安装包内的 `dclassql/client.py`, 模型文件通过 `generated_models` 目录中的符号链接或备份副本暴露。

## 数据模型解析
- `model_inspector.inspect_models`:
  - 使用 `get_type_hints`、`fields` 等 API 解析 dataclass 字段, 判断可选性、默认值、自增主键等。
  - 借助 `table_spec.TableInfo` 的 fake self 机制处理 `primary_key` / `index` / `unique_index` 方法返回的列定义。
  - 将模型按 `__datasource__` 聚合为 `DataSourceConfig`, key 为 `name` 或 provider。
- 关系与外键:
  - `RelationAttribute`/`RelationProxy`/`ForeignKeyComparison` 支持 `self.user.id == self.user_id` 等表达式, 映射为关系和键约束。
  - 收集 `ForeignKeyInfo` 以驱动运行时 include 与懒加载。

## 运行时与数据库推送
- `push.db_push`:
  - 按 provider 和 datasource key 分组模型, 委派给对应的 `DatabasePusher`。
  - 默认注册 `SQLitePusher`, 支持外部 `register_pusher` 扩展。
- `push/sqlite.py`:
  - `_infer_sqlite_type` 将 Python 类型映射为 SQLite 列类型, 处理 `Annotated`/`Union`。
  - `SQLiteSchemaBuilder` 生成 `CREATE TABLE` 语句、索引定义, 支持自增主键内联定义。
  - `SQLitePusher` 能检查现有 schema, 根据 `SchemaDiff` 判定是否需要重建表; 重建时会创建临时表迁移数据, 并可通过 `confirm_rebuild` 回调确认。
  - `sync_indexes=True` 会删除多余索引/重建缺失索引。
- `runtime/backends.base.BackendBase`:
  - 定义通用 CRUD 实现, 支持 typed insert payload (dataclass、TypedDict、Mapping)。
  - 提供 identity map 与关系懒加载 (`ensure_lazy_state`/`resolve_lazy_relation`)。
- `runtime/backends.sqlite.SQLiteBackend`:
  - 基于 sqlite3, 可接受连接或线程局部工厂, 实现批量插入、`query_raw`/`execute_raw`。
  - 默认使用 `sqlite3.Row` 作为 row factory, 保证列名访问。
- `runtime/backends.lazy`:
  - 定义懒加载代理 (`_LazyRelationDescriptor`, `_LazyListProxy` 等) 与 `eager()` 帮助函数。
  - `LazyRelationState` 维护加载状态、映射关系, 支持一对一/一对多访问及 backref。
- `runtime/datasource.py` + `sqlite_adapters.py`: 解析 `sqlite:///...` URL, 注册日期/时间适配器, 构造连接。

## CLI 与工具链
- 安装后提供 `dql` 命令 (在 `pyproject.toml` 注册)。
- `dql -m model.py generate`:
  - 载入模型模块 (`importlib.util`), 收集 dataclass, 调整 `__module__` 以生成 typed 代码。
  - 写入生成客户端, 同时在 `generated_models/` 下维护模型文件的符号链接/副本。
- `dql -m model.py push-db`:
  - 载入模型后, 为每个 datasource 打开连接 (`runtime.datasource.open_sqlite_connection`)。
  - 调用 `db_push` 应用 schema 与索引。
- CLI 出错直接抛异常; 统一由 `main()` 捕获并写至 stderr。

## 辅助模块与工具
- `db_pool.BaseDBPool` + `save_local` 装饰器: 在线程局部缓存数据库连接/对象, 提供 `close_all()` 释放资源。
- `typing.py` 定义生成器使用的泛型 TypeVar (`ModelT`, `InsertT` 等)。
- `unwarp.py` 提供 `unwarp_or`/`unwarp_or_raise` 等辅助函数, 在生成代码中用于处理可空值。
- `generated_models/exchange_info.py`: 存放示例 dataclass 模型 (交易所信息) 供测试/演示。

## 测试与质量保障
- 使用 `pytest` (见 `tests/`) 覆盖:
  - `test_codegen*`: 校验生成代码结构、上下文数据、导出内容。
  - `test_sqlite_push.py`: 验证 schema/索引生成与重建逻辑、diff 报告。
  - `test_runtime_sqlite.py`: 集成测试 CRUD、懒加载、线程安全、批量插入与错误分支。
  - `test_cli.py`: 检查 `dql` 命令输出、生成文件与 push-db 功能。
  - `test_typecheck.py`: 调用 `uv run pyright` 验证类型错误能被检测。
- `tests/results.py` 存放期望的生成代码快照 (持续更新)。
- 使用 `uv run pyright .` 检查项目类型, 项目不大, 不用特地只检查单独的文件, 每次都检查项目库

## 代码风格
- 少多 Any 和 cast, 多用泛型, 特别是3.12+的泛型语法`def f[T](x: T, y: T) -> T: ...`
- 要有大局观, 尽量少重复实现

## 设计
- 检查 @TARGET.md

## 当前能力边界与路线
- 仅支持 SQLite; 其他数据库需实现自定义 `DatabasePusher` 与运行时 Backend。
- 查询 API 聚焦基本 CRUD, 未来计划扩展更复杂的查询能力和更多数据库驱动 (见 `TARGET.md` 路线图)。
- 外键在数据库层面仍为“虚拟外键”(不创建真实约束), 依赖运行时约定。
- 包含机制目前基于懒加载与 `include` bool map, 不支持复杂嵌套查询条件。

## 最新进展
- where 子句支持 Prisma 风格的标量过滤、AND/OR/NOT 组合, 并新增 `*RelationFilter` 结构及运行时子查询编译, 可表达 `IS/IS_NOT/SOME/NONE/EVERY` 等关系筛选。
- `WhereCompiler` 抽离为独立模块, 统一负责 SQL 条件生成。
- 代码生成自动导出对应 RelationFilter 类型, 测试覆盖已更新。
- `Client` 与 backend 增加 `echo_sql` 参数, 所有 SQL 通过标准化执行入口 `_execute_sql` 处理并可统一日志输出。
