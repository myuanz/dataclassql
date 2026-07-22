# Changelog

本项目从 `0.3.1` 开始记录变更。

## 0.5.0 - 2026-07-22
- 模型解析与客户端代码生成重构为 `ModelGraph -> ClientCompiler -> GeneratedModule` 流水线，优化代码结构。
- `foreign_key()` 改为在模型代理组成的新 globals 中解析, 不再临时替换原始 dataclass 字段。
- 新增 `TypeHint` 包装 Python type 的 `source`、`origin`、`args`, 提供通用的剥壳方法; 
- 澄清类型约束: 
  - JSON 值支持嵌套 dataclass、TypedDict、标量 list/dict 与异构 tuple
  - 关系字段仅接受 dataclass 或 `list[dataclass]`
  - 类型标注拒绝非 Optional Union、`set`、`frozenset` 和非字符串 dict key。
- 关系运行时元信息统一为 `TableRelation`, 懒加载和关系过滤共享同一份列映射。
- 多值关系保持 `list[T]` 声明; 未 include 时返回只读 `LazyRelationView`, 支持存在性、计数、按索引查询及显式完整迭代, include 后返回真正的 `list[T]`。
- lazy proxy 使用 `LazyLookupKey` 判断查询来源相等, proxy 相等和 hash 不再隐式查询关系内容。
- lazy 关系状态改用基于 `id(instance)` 的弱引用 registry, 不再修改模型 `__hash__`; include 结果和主动赋值直接写入关系字段并解除 lazy 绑定。
- Mapping 写入不再静默丢弃未知列, 由数据库统一报告无效列错误。
- `order_by` 使用固定表别名限定动态列名并由 PyPika 转义, 使 SQLite 能报告未知列且避免 SQL 注入。
- 数据库列可空性仅由 `T | None` / `Optional[T]` 决定, dataclass 的 default 和 default_factory 只影响生成的插入对象默认值。
- 唯一索引仅生成命名的 `CREATE UNIQUE INDEX`, 不再同时创建表级 `UNIQUE` 和重复的 `sqlite_autoindex_*`。
- SQLite 建表和重建会在同一事务内创建全部模型索引; 模型外索引不随重建保留。

## 0.4.2 - 2026-07-18

### Added

- `foreign_key()` 支持将 backref 显式写为 `None`, 用于只保留本模型访问入口的单向关系。

### Changed

- backref 仅接受目标模型上的关系属性或 `None`; 其他值会直接抛出 `TypeError`, 不再静默视为无 backref。
- 扩展生成客户端中的 `ColumnSpec`，方便 push db 使用。
- `client.push_db()` 与 CLI `push-db` 不再重复解析 model，默认信任 client 存在。
- 生成 Client 改为继承 `ClientBase`, 通用连接、backend、推送和关闭逻辑不再写入生成文件。
- `generate` 增加 `--push-db`, 可在生成客户端后立即推送数据库 schema。

## 0.4.1 - 2026-07-05

### Added

- 支持未写 `primary_key()` 且模型没有 `id` 字段时自动创建隐式自增 `id` 主键列; 生成客户端会在 `Insert`、`Where`、`OrderBy`、upsert where 等间接描述里暴露该 `id`, 查询和写入返回值仍保持原 dataclass 字段。
- 支持通过 `__exclude__` 排除辅助 dataclass, 未收集为模型的 dataclass 字段会按 JSON 值对象存储并在读取时还原。
- 生成客户端保留模型中的类型别名与默认值语义, 包括 PEP 695 type alias 和 dataclass default/default_factory。
- `generate` 支持从项目根路径导入模型模块。

### Fixed

- 修复 slotted 模型的普通关系对象在 lazy descriptor 安装后访问关系字段时报 `__dict__` 缺失的问题。
- 修复关系映射按属性匹配不精确导致多关系模型可能串错映射的问题。
- 修复 SQLite 类型推断对类型别名处理不完整的问题。
- 对 `slots=True` 模型显式要求 `weakref_slot=True`, 避免运行时 lazy/identity map 需要弱引用时才暴露错误。

### Changed

- 重构 datasource 与 `db_push` 连接路径, 生成客户端的内存 datasource 连接复用更稳定。
- `save_local` 连接缓存调整为实例级作用域, 避免不同客户端实例共享不该共享的连接。

## 0.4.0 - 2026-07-04

### Added

- `dclassql generate` 默认按 model 文件生成同名 `_client` 包目录, 例如 `user_model.py` 生成 `user_model_client/`。
- `generate` 增加 `--target {model-dir,package}` 选项, 可选择生成到 model 同目录或 `dclassql` 包内。
- 每个生成客户端包包含 `client.py`、`asdict.pyi`、`__init__.py` 与 `__init__.pyi`, 并导出按 model 文件命名的客户端类。
- 生成客户端的 `push_db()` 增加 `force_rebuild` 参数, 默认拒绝不兼容重建, 传入 `True` 时自动允许重建。

### Changed

- typed `asdict` stub 从全局 `dclassql/asdict.pyi` 迁移到每个生成客户端包内。
- 移除旧的 `dclassql.Client` 包级生成入口, 生成客户端改为通过各自的 `_client` 包导入。

## 0.3.1 - 2026-06-30

### Added

- 支持在构造生成客户端时覆盖 datasource，例如 `Client(datasource=DataSourceConfig(...))`。
- 支持通过生成客户端实例调用 `client.push_db()`，使用同一份运行时 datasource 配置推送数据库 schema。

### Changed

- 生成客户端从固定模块级 datasource 配置调整为单客户端单 datasource 配置，便于后续按模型模块生成独立客户端。
- 运行时连接和 backend 构造改为通过通用 datasource/provider 分派路径处理，避免表访问层直接依赖固定的 sqlite-only 缓存字段。
