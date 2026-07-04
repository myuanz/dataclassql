# Changelog

本项目从 `0.3.1` 开始记录变更。

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
