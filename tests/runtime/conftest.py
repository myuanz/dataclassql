from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum, IntEnum
from pathlib import Path
from typing import Any, cast

import pytest

from dclassql.codegen import generate_client
from dclassql.model_inspector import DataSourceConfig

__datasource__ = {"url": "sqlite:///:memory:"}


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
    module = generate_client([RuntimeUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    client = namespace[module.client_class_name](datasource=DataSourceConfig(url=__datasource__["url"]))
    try:
        client.push_db()
        client.runtime_user.delete_many()
    finally:
        client.close()


def build_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    namespace["__client_class_name__"] = module.client_class_name
    generated_client = namespace[module.client_class_name]
    client = generated_client()
    return namespace, client


def build_enum_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeEnumUser])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    namespace["__client_class_name__"] = module.client_class_name
    client = namespace[module.client_class_name]()
    return namespace, client


@pytest.fixture(autouse=True)
def cleanup_clients():
    yield
