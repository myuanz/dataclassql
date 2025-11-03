from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def resolve_sqlite_path(url: str | None) -> str:
    if not url:
        raise ValueError("SQLite datasource must specify a url, e.g. sqlite:///path/to.db")

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
        resolved = Path(path[1:])
    elif path.startswith("/"):
        resolved = Path(path[1:])
    else:
        resolved = Path(path)
    return resolved.as_posix()


def open_sqlite_connection(url: str | None) -> sqlite3.Connection:
    path = resolve_sqlite_path(url)
    return sqlite3.connect(path, check_same_thread=False)
