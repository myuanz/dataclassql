from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from dclassql.codegen import generate_client

__datasource__ = {"provider": "sqlite", "url": None}

@dataclass
class User:
    id: int
    name: str
    email: str


@pytest.mark.skipif(os.environ.get("SKIP_PYRIGHT_TESTS") == "1", reason="pyright check skipped")
def test_pyright_reports_missing_required_field(tmp_path: Path) -> None:
    db_path = tmp_path / "pyright.db"

    global __datasource__
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"}


    module = generate_client([User])
    client_path = tmp_path / "client_module.py"
    client_path.write_text(module.code, encoding="utf-8")

    snippet = tmp_path / "snippet.py"
    snippet.write_text(
        """from .client_module import Client

client = Client()
client.user.insert({"name": "Alice", "email": "a@example.com"})
client.user.insert({"email": "missing"})
""",
        encoding="utf-8",
    )

    result = subprocess.run(
        ["uv", "run", "pyright", str(snippet), "--verbose", ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0

    assert 'dict[str, str]' in result.stdout and 'UserInsertDict' in result.stdout, result.stdout
    assert 'reportArgumentType' in result.stdout, result.stdout
    assert 'is not assignable to' in result.stdout, result.stdout
    assert 'snippet.py:5:20 - error:' in result.stdout, result.stdout
