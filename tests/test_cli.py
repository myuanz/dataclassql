from __future__ import annotations

import sqlite3
from pathlib import Path
from enum import Enum

import pytest

from dclassql.cli import (
    DEFAULT_MODEL_FILE,
    main,
    resolve_asdict_stub_path,
    resolve_generated_path,
)


MODEL_TEMPLATE = """
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

__datasource__ = {{
    "provider": "sqlite",
    "url": "sqlite:///{db_path}",
    "name": {datasource_name!r},
}}

@dataclass
class User:
    id: int
    name: str
    email: str | None
    created_at: datetime

    def index(self):
        yield self.name
        yield self.created_at
"""

ENUM_MODEL_TEMPLATE = """
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

__datasource__ = {{
    "provider": "sqlite",
    "url": "sqlite:///{db_path}",
    "name": {datasource_name!r},
}}

class RunStatus(Enum):
    PENDING = "pending"
    DONE = "done"

@dataclass
class RunRecord:
    id: int
    status: RunStatus
"""


def write_model(tmp_path: Path, db_path: Path, name: str | None = None) -> Path:
    module_path = tmp_path / DEFAULT_MODEL_FILE
    module_path.write_text(
        MODEL_TEMPLATE.format(
            db_path=db_path.as_posix(),
            datasource_name=name if name is not None else "None",
        ),
        encoding="utf-8",
    )
    return module_path


def write_enum_model(tmp_path: Path, db_path: Path, name: str | None = None) -> Path:
    module_path = tmp_path / "enum_model.py"
    module_path.write_text(
        ENUM_MODEL_TEMPLATE.format(
            db_path=db_path.as_posix(),
            datasource_name=name if name is not None else "None",
        ),
        encoding="utf-8",
    )
    return module_path


def test_generate_command_outputs_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "example.db"
    module_path = write_model(tmp_path, db_path)
    target = resolve_generated_path()
    stub_target = resolve_asdict_stub_path()
    backup = target.read_text(encoding="utf-8") if target.exists() else None
    stub_backup = stub_target.read_text(encoding="utf-8") if stub_target.exists() else None
    exit_code = main(["-m", str(module_path), "generate"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert str(target) in captured.out
    assert target.exists()
    assert stub_target.exists()
    code = target.read_text(encoding="utf-8")
    assert "class Client" in code
    assert "class UserTable" in code
    stub_code = stub_target.read_text(encoding="utf-8")
    assert "RelationPolicy" in stub_code

    if backup is None:
        target.unlink(missing_ok=True)
    else:
        target.write_text(backup, encoding="utf-8")
    if stub_backup is None:
        stub_target.unlink(missing_ok=True)
    else:
        stub_target.write_text(stub_backup, encoding="utf-8")


def test_generate_command_rebinds_enum_imports(tmp_path: Path) -> None:
    db_path = tmp_path / "enum.db"
    module_path = write_enum_model(tmp_path, db_path, name="enum")
    target = resolve_generated_path()
    stub_target = resolve_asdict_stub_path()
    backup = target.read_text(encoding="utf-8") if target.exists() else None
    stub_backup = stub_target.read_text(encoding="utf-8") if stub_target.exists() else None
    exit_code = main(["-m", str(module_path), "generate"])
    assert exit_code == 0
    code = target.read_text(encoding="utf-8")
    assert "RunRecord" in code
    stub_code = stub_target.read_text(encoding="utf-8")
    assert "RunRecordDict" in stub_code

    if backup is None:
        target.unlink(missing_ok=True)
    else:
        target.write_text(backup, encoding="utf-8")
    if stub_backup is None:
        stub_target.unlink(missing_ok=True)
    else:
        stub_target.write_text(stub_backup, encoding="utf-8")


def test_push_db_command_creates_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "push.sqlite"
    module_path = write_model(tmp_path, db_path, name="main")
    exit_code = main(["-m", str(module_path), "push-db"])
    assert exit_code == 0

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='User'"
        ).fetchone()[0]
        assert count == 1
    finally:
        conn.close()


def test_push_db_command_confirms_rebuild_auto(tmp_path: Path) -> None:
    db_path = tmp_path / "auto.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            'CREATE TABLE "User" ("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL)'
        )
    finally:
        conn.close()

    module_path = write_model(tmp_path, db_path, name="auto")
    exit_code = main(["-m", str(module_path), "push-db", "--confirm-rebuild", "auto"])
    assert exit_code == 0

    conn = sqlite3.connect(db_path)
    try:
        columns = conn.execute('PRAGMA table_info("User")').fetchall()
    finally:
        conn.close()

    column_names = [name for (_cid, name, *_rest) in columns]
    assert column_names == ["id", "name", "email", "created_at"]


def test_push_db_command_prompt_rebuild_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "prompt.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            'CREATE TABLE "User" ("id" INTEGER PRIMARY KEY AUTOINCREMENT,"name" TEXT NOT NULL)'
        )
    finally:
        conn.close()

    module_path = write_model(tmp_path, db_path, name="prompt")
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")
    exit_code = main(["-m", str(module_path), "push-db", "--confirm-rebuild", "prompt"])
    assert exit_code == 1

    captured = capsys.readouterr()
    assert "需要重建表" in captured.err

    conn = sqlite3.connect(db_path)
    try:
        columns = conn.execute('PRAGMA table_info("User")').fetchall()
    finally:
        conn.close()

    column_names = [name for (_cid, name, *_rest) in columns]
    assert column_names == ["id", "name"]


def test_push_db_command_sync_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "sync.sqlite"
    module_path = write_model(tmp_path, db_path, name="sync")
    assert main(["-m", str(module_path), "push-db"]) == 0

    conn = sqlite3.connect(db_path)
    try:
        conn.execute('CREATE INDEX "idx_User_extra" ON "User" ("created_at", "name")')
        conn.execute('DROP INDEX "idx_User_name"')
    finally:
        conn.close()

    exit_code = main(["-m", str(module_path), "push-db", "--sync-indexes"])
    assert exit_code == 0

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            'SELECT name FROM sqlite_master WHERE type="index" AND tbl_name="User"'
        ).fetchall()
    finally:
        conn.close()

    index_names = {name for (name,) in rows if not name.startswith("sqlite_")}
    assert index_names == {"idx_User_name", "idx_User_created_at"}
