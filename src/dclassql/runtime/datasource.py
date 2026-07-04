from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .sqlite_adapters import register_sqlite_adapters

def resolve_sqlite_path(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "sqlite":
        raise ValueError(f"Unsupported sqlite url '{url}'")

    if parsed.path in {":memory:", "/:memory:"}:
        return ":memory:"

    if parsed.netloc:
        raise ValueError(f"Unsupported sqlite netloc '{parsed.netloc}' in url '{url}'")

    path = parsed.path
    if not path:
        return ":memory:"

    if path.startswith("//"):
        target = Path(path[1:])
        return target.as_posix()

    if path.startswith("/"):
        path = path[1:]
    target = Path(path)
    return target.as_posix()


def open_sqlite_connection(url: str) -> sqlite3.Connection:
    path = resolve_sqlite_path(url)
    register_sqlite_adapters()
    return sqlite3.connect(
        path,
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
