from __future__ import annotations

import sqlite3
import shutil
from pathlib import Path
from enum import Enum

import pytest

from dclassql.cli import (
    DEFAULT_MODEL_FILE,
    collect_models,
    load_module,
    main,
    resolve_client_class_name,
    resolve_generated_package_dir,
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
            datasource_name=name,
        ),
        encoding="utf-8",
    )
    return module_path


def write_enum_model(tmp_path: Path, db_path: Path, name: str | None = None) -> Path:
    module_path = tmp_path / "enum_model.py"
    module_path.write_text(
        ENUM_MODEL_TEMPLATE.format(
            db_path=db_path.as_posix(),
            datasource_name=name,
        ),
        encoding="utf-8",
    )
    return module_path


def test_generate_command_outputs_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "example.db"
    module_path = write_model(tmp_path, db_path)
    target_dir = resolve_generated_package_dir(module_path)
    target = target_dir / "client.py"
    stub_target = target_dir / "asdict.pyi"
    exit_code = main(["-m", str(module_path), "generate"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert str(target_dir) in captured.out
    assert target.exists()
    assert stub_target.exists()
    assert (target_dir / "__init__.py").exists()
    assert (target_dir / "__init__.pyi").exists()
    code = target.read_text(encoding="utf-8")
    assert f"class {resolve_client_class_name(module_path)}" in code
    assert "class UserTable" in code
    stub_code = stub_target.read_text(encoding="utf-8")
    assert "RelationPolicy" in stub_code


def test_generate_command_rebinds_enum_imports(tmp_path: Path) -> None:
    db_path = tmp_path / "enum.db"
    module_path = write_enum_model(tmp_path, db_path, name="enum")
    exit_code = main(["-m", str(module_path), "generate"])
    assert exit_code == 0
    target_dir = resolve_generated_package_dir(module_path)
    target = target_dir / "client.py"
    stub_target = target_dir / "asdict.pyi"
    code = target.read_text(encoding="utf-8")
    assert "RunRecord" in code
    stub_code = stub_target.read_text(encoding="utf-8")
    assert "RunRecordDict" in stub_code


def test_generate_command_loads_project_root_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir = tmp_path / "project"
    utils_dir = project_dir / "src" / "utils"
    model_dir = project_dir / "src" / "models"
    utils_dir.mkdir(parents=True)
    model_dir.mkdir(parents=True)
    (project_dir / "src" / "__init__.py").write_text("", encoding="utf-8")
    (utils_dir / "__init__.py").write_text("", encoding="utf-8")
    (model_dir / "__init__.py").write_text("", encoding="utf-8")
    (utils_dir / "names.py").write_text("DEFAULT_NAME = 'Alice'\n", encoding="utf-8")
    module_path = model_dir / "model.py"
    module_path.write_text(
        """
from dataclasses import dataclass

from src.utils.names import DEFAULT_NAME

__datasource__ = {"provider": "sqlite", "url": "sqlite:///example.db"}

@dataclass
class User:
    id: int
    name: str = DEFAULT_NAME
""".lstrip(),
        encoding="utf-8",
    )

    monkeypatch.chdir(project_dir)
    exit_code = main(["-m", str(module_path), "generate"])

    assert exit_code == 0
    code = (model_dir / "model_client" / "client.py").read_text(encoding="utf-8")
    assert "class ModelClient" in code
    assert "name: str = User.__dataclass_fields__['name'].default" in code


def test_collect_models_honors_module_exclude(tmp_path: Path) -> None:
    module_path = tmp_path / "model.py"
    module_path.write_text(
        """
from dataclasses import dataclass

__datasource__ = {"provider": "sqlite", "url": "sqlite:///example.db"}

@dataclass
class Stamp:
    idx: int

@dataclass
class Event:
    id: int
    stamp: Stamp

__exclude__ = (Stamp,)
""".lstrip(),
        encoding="utf-8",
    )

    module = load_module(module_path)
    models = collect_models(module)

    assert [model.__name__ for model in models] == ["Event"]


def test_generate_command_supports_package_target(tmp_path: Path) -> None:
    db_path = tmp_path / "package.db"
    module_path = write_model(tmp_path, db_path, name="package")
    target_dir = resolve_generated_package_dir(module_path, "package")
    try:
        exit_code = main(["-m", str(module_path), "generate", "--target", "package", "--push-db"])
        assert exit_code == 0
        assert (target_dir / "__init__.py").exists()
        assert (target_dir / "__init__.pyi").exists()
        assert (target_dir / "client.py").exists()
        assert (target_dir / "asdict.pyi").exists()
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='User'"
            ).fetchone()[0] == 1
        finally:
            conn.close()
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)


def test_push_db_command_creates_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "push.sqlite"
    module_path = write_model(tmp_path, db_path, name="main")
    exit_code = main(["-m", str(module_path), "generate", "--push-db"])
    assert exit_code == 0

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='User'"
        ).fetchone()[0]
        assert count == 1
    finally:
        conn.close()


def test_push_db_command_requires_generated_client(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module_path = write_model(tmp_path, tmp_path / "missing-client.sqlite")

    assert main(["-m", str(module_path), "push-db"]) == 1
    assert "run generate first" in capsys.readouterr().err


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
    assert main(["-m", str(module_path), "generate"]) == 0
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
    assert main(["-m", str(module_path), "generate"]) == 0
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
    assert main(["-m", str(module_path), "generate"]) == 0
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
