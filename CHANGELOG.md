# Changelog

本项目从 `0.3.1` 开始记录变更。

## 0.3.1 - 2026-06-30

### Added

- 支持在构造生成客户端时覆盖 datasource，例如 `Client(datasource=DataSourceConfig(...))`。
- 支持通过生成客户端实例调用 `client.push_db()`，使用同一份运行时 datasource 配置推送数据库 schema。

### Changed

- 生成客户端从固定模块级 datasource 配置调整为单客户端单 datasource 配置，便于后续按模型模块生成独立客户端。
- 运行时连接和 backend 构造改为通过通用 datasource/provider 分派路径处理，避免表访问层直接依赖固定的 sqlite-only 缓存字段。
