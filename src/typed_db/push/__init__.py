from __future__ import annotations

from typing import Any, Mapping, Sequence

from ..model_inspector import ModelInfo, inspect_models
from .base import DatabasePusher
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
) -> None:
    model_infos = inspect_models(models)
    grouped: dict[str, list[ModelInfo]] = {}
    for info in model_infos.values():
        grouped.setdefault(info.datasource.provider, []).append(info)

    for provider, infos in grouped.items():
        if provider not in connections:
            raise KeyError(f"Connection for provider '{provider}' is missing")
        pusher = get_pusher(provider)
        connection = connections[provider]
        pusher.push(connection, infos, sync_indexes=sync_indexes)


__all__ = [
    "db_push",
    "get_pusher",
    "register_pusher",
    "SQLitePusher",
    "push_sqlite",
    "_build_sqlite_schema",
]
