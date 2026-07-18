from __future__ import annotations

from typing import Any, Sequence

from ..runtime.backends.protocols import SchemaTableProtocol
from .base import ConfirmRebuildCallback, DatabasePusher
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
    tables: Sequence[SchemaTableProtocol],
    connection: Any,
    *,
    provider: str,
    sync_indexes: bool = False,
    confirm_rebuild: ConfirmRebuildCallback | None = None,
) -> None:
    pusher = get_pusher(provider)
    pusher.push(
        connection,
        tables,
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
