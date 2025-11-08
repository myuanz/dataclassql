from __future__ import annotations

from dataclasses import dataclass, asdict as dataclass_asdict
from pathlib import Path
from typing import Any

from dclassql import asdict
from dclassql.codegen import generate_client
from dclassql.push import db_push
from dclassql.runtime.datasource import open_sqlite_connection


__datasource__ = {"provider": "sqlite", "url": None}


@dataclass
class RuntimeAuthor:
    id: int
    name: str
    profile: RuntimeProfile | None
    posts: list[RuntimePost]

    def primary_key(self) -> int:
        return self.id


@dataclass
class RuntimeProfile:
    id: int
    user_id: int
    bio: str
    user: RuntimeAuthor

    def primary_key(self) -> int:
        return self.id

    def foreign_key(self):
        yield self.user.id == self.user_id, RuntimeAuthor.profile


@dataclass
class RuntimePost:
    id: int
    user_id: int
    title: str
    user: RuntimeAuthor

    def primary_key(self) -> int:
        return self.id

    def foreign_key(self):
        yield self.user.id == self.user_id, RuntimeAuthor.posts


def _prepare_database(db_path: Path) -> None:
    global __datasource__
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"}
    conn = open_sqlite_connection(__datasource__["url"])
    try:
        db_push([RuntimeAuthor, RuntimeProfile, RuntimePost], {"sqlite": conn})
    finally:
        conn.close()


def _build_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeAuthor, RuntimeProfile, RuntimePost])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    client_cls = namespace["Client"]
    return namespace, client_cls()


def test_asdict_handles_lazy_relations(tmp_path: Path) -> None:
    db_path = tmp_path / "asdict.sqlite"
    _prepare_database(db_path)
    namespace, client = _build_client()

    author_table = client.runtime_author
    profile_table = client.runtime_profile
    post_table = client.runtime_post

    AuthorInsert = namespace["RuntimeAuthorInsert"]
    ProfileInsert = namespace["RuntimeProfileInsert"]
    PostInsert = namespace["RuntimePostInsert"]

    author = author_table.insert(AuthorInsert(id=1, name="Alice"))
    profile_table.insert(ProfileInsert(id=10, user_id=author.id, bio="hi"))
    post_table.insert(PostInsert(id=20, user_id=author.id, title="first"))
    post_table.insert(PostInsert(id=21, user_id=author.id, title="second"))

    fetched = author_table.find_first(where={"id": author.id})
    assert fetched is not None

    base_result = asdict(fetched)
    assert base_result["profile"] is None
    assert base_result["posts"] == []

    fetched_result = asdict(fetched, relation_policy="fetch")
    assert fetched_result["profile"]["bio"] == "hi"
    assert {entry["title"] for entry in fetched_result["posts"]} == {"first", "second"}

    keep_result = asdict(fetched)
    assert keep_result["profile"]["bio"] == "hi"
    assert len(keep_result["posts"]) == 2

    skip_result = asdict(fetched, relation_policy="skip")
    assert skip_result["profile"] is None
    assert skip_result["posts"] == []

    sequence_result = asdict([fetched])
    assert isinstance(sequence_result, list)
    assert sequence_result[0] == keep_result

    assert dataclass_asdict(fetched) == keep_result

    client.__class__.close_all()
