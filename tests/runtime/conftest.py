from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum, IntEnum
from pathlib import Path
from typing import Any, cast

import pytest

from dclassql.codegen import generate_client
from dclassql.push import db_push
from dclassql.runtime.datasource import open_sqlite_connection

__datasource__ = {"provider": "sqlite", "url": None}


@dataclass
class RuntimeUser:
    id: int
    name: str
    email: str | None


class RuntimeState(Enum):
    ACTIVE = "active"
    DISABLED = "disabled"


class StrEnumTest(StrEnum):
    FIRST = "first"
    SECOND = "second"


class IntEnumTest(IntEnum):
    ONE = 1
    TWO = 2


@dataclass
class RuntimeEnumUser:
    id: int
    state: RuntimeState
    s1: StrEnumTest
    s2: IntEnumTest


def prepare_database(db_path: Path) -> None:
    global __datasource__
    if db_path.exists():
        db_path.unlink()
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"}
    conn = open_sqlite_connection(__datasource__["url"])
    try:
        db_push([RuntimeUser], {"sqlite": conn})
        conn.execute('DELETE FROM "RuntimeUser"')
        conn.commit()
    finally:
        conn.close()


def build_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    generated_client = namespace["Client"]
    client = generated_client()
    return namespace, client


def build_enum_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeEnumUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    client = namespace["Client"]()
    return namespace, client


@pytest.fixture(autouse=True)
def cleanup_clients():
    yield
    # best effort close all generated clients if present
    cls = getattr(sys.modules.get("dclassql.client", None), "Client", None)
    if cls and hasattr(cls, "close_all"):
        try:
            cls.close_all()
        except Exception:
            pass
