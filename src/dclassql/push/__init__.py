from __future__ import annotations

from typing import Any, Callable, Sequence

from ..model_inspector import ModelInfo, inspect_models
from .base import DatabasePusher, ExistingColumn, SchemaDiff, SchemaPlan
from .sqlite import SQLITE_PUSHER, SQLitePusher, push_sqlite, _build_sqlite_schema

_PUSHER_REGISTRY: dict[str, DatabasePusher] = {
    "sqlite": SQLITE_PUSHER,
}


def register_pusher(provider: str, pusher: DatabasePusher) -> None:
    _PUSHER_REGISTRY[provider] = pusher


def get_pusher(provider: str) -> DatabasePusher:
    if provider not in _PUSHER_REGISTRY:
        raise ValueError(f"Unsupported provider: {provider}")
    return _PUSHER_REGISTRY[provider]


def db_push(
    models: Sequence[type[Any]],
    connection: Any,
    *,
    sync_indexes: bool = False,
    confirm_rebuild: Callable[[ModelInfo, SchemaPlan, tuple[ExistingColumn, ...] | None, SchemaDiff], bool] | None = None,
) -> None:
    model_infos = inspect_models(models)
    datasource_configs = {info.datasource for info in model_infos.values()}
    if len(datasource_configs) != 1:
        labels = ", ".join(sorted(config.identity for config in datasource_configs))
        raise ValueError(f"db_push only supports one datasource, got: {labels}")
    datasource = next(iter(datasource_configs))
    pusher = get_pusher(datasource.provider)
    pusher.push(
        connection,
        list(model_infos.values()),
        sync_indexes=sync_indexes,
        confirm_rebuild=confirm_rebuild,
    )


__all__ = [
    "db_push",
    "get_pusher",
    "register_pusher",
    "SQLitePusher",
    "push_sqlite",
    "_build_sqlite_schema",
]
