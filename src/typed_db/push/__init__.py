from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence

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
    connections: Mapping[str, Any],
    *,
    sync_indexes: bool = False,
    confirm_rebuild: Callable[[ModelInfo, SchemaPlan, tuple[ExistingColumn, ...] | None, SchemaDiff], bool] | None = None,
) -> None:
    model_infos = inspect_models(models)
    grouped: dict[str, dict[str, list[ModelInfo]]] = {}
    for info in model_infos.values():
        provider = info.datasource.provider
        key = info.datasource.key
        provider_map = grouped.setdefault(provider, {})
        provider_map.setdefault(key, []).append(info)

    for provider, key_map in grouped.items():
        pusher = get_pusher(provider)
        for key, infos in key_map.items():
            if key not in connections:
                raise KeyError(f"Connection for datasource '{key}' is missing")
            connection = connections[key]
            pusher.push(
                connection,
                infos,
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
