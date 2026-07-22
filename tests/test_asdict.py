from __future__ import annotations

import gc
from dataclasses import dataclass, field, asdict as dataclass_asdict
from pathlib import Path
from typing import Any, cast
from weakref import ref

from dclassql import asdict, record_sql
from dclassql.codegen import generate_client
from dclassql.model_inspector import DataSourceConfig
from dclassql.runtime.backends.lazy import (
    LAZY_RELATION_REGISTRY,
    LazyRelationState,
    _LazyRelationRegistry,
)


__datasource__ = {"url": "sqlite:///:memory:"}


@dataclass
class RuntimeAuthor:
    id: int
    name: str
    profile: RuntimeProfile | None
    posts: list[RuntimePost]
    metadata: dict[str, int] = field(default_factory=dict)

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

    def unique_index(self):
        return self.user_id

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


_RUNTIME_AUTHOR_HASH = RuntimeAuthor.__hash__


class FactoryDict(dict[str, Any]):
    pass


def test_asdict_supports_plain_unhashable_dataclass() -> None:
    @dataclass
    class Plain:
        value: int
        metadata: dict[str, int]

    value = Plain(1, {"rank": 2})
    assert asdict(value) == {"value": 1, "metadata": {"rank": 2}}
    assert dataclass_asdict(value) == {"value": 1, "metadata": {"rank": 2}}


def test_lazy_relation_registry_uses_identity_and_releases_instances() -> None:
    @dataclass
    class EqualValue:
        value: int

    registry = _LazyRelationRegistry()
    first = EqualValue(1)
    second = EqualValue(1)
    first_state = LazyRelationState(
        "relation",
        cast(Any, None),
        cast(Any, object),
        {},
        True,
    )
    second_state = LazyRelationState(
        "relation",
        cast(Any, None),
        cast(Any, object),
        {},
        True,
    )

    registry.bind(first, first_state)
    registry.bind(second, second_state)

    assert first == second
    first_states = registry.get(first)
    second_states = registry.get(second)
    assert first_states is not None and first_states["relation"] is first_state
    assert second_states is not None and second_states["relation"] is second_state
    assert len(registry) == 2

    first_ref = ref(first)
    del first
    gc.collect()
    assert first_ref() is None
    assert len(registry) == 1


def _prepare_database(db_path: Path) -> None:
    global __datasource__
    __datasource__ = {"provider": "sqlite", "url": f"sqlite:///{db_path.as_posix()}"}
    module = generate_client([RuntimeAuthor, RuntimeProfile, RuntimePost])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    client = namespace[module.client_class_name](datasource=DataSourceConfig(url=__datasource__["url"]))
    try:
        client.push_db()
    finally:
        client.close()


def _build_client() -> tuple[dict[str, Any], Any]:
    module = generate_client([RuntimeAuthor, RuntimeProfile, RuntimePost])
    namespace: dict[str, Any] = {}
    exec(module.code, namespace)
    client_cls = namespace[module.client_class_name]
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

    author = author_table.insert(
        AuthorInsert(id=1, name="Alice", metadata={"rank": 1})
    )
    profile_table.insert(ProfileInsert(id=10, user_id=author.id, bio="hi"))
    post_table.insert(PostInsert(id=20, user_id=author.id, title="first"))
    post_table.insert(PostInsert(id=21, user_id=author.id, title="second"))

    fetched = author_table.find_first(where={"id": author.id})
    assert fetched is not None
    assert _RUNTIME_AUTHOR_HASH is None
    assert RuntimeAuthor.__hash__ is _RUNTIME_AUTHOR_HASH
    fetched_state = LAZY_RELATION_REGISTRY.get(fetched)
    assert fetched_state is not None
    assert set(fetched_state) == {"profile", "posts"}

    base_result = asdict(fetched)
    assert base_result["profile"] is None
    assert base_result["posts"] == []
    assert base_result["metadata"] == {"rank": 1}

    fetched_result = asdict(fetched, relation_policy="fetch")
    assert fetched_result["profile"]["bio"] == "hi"
    assert {entry["title"] for entry in fetched_result["posts"]} == {"first", "second"}

    keep_result = asdict(fetched)
    assert keep_result["profile"] is None
    assert keep_result["posts"] == []

    skip_result = asdict(fetched, relation_policy="skip")
    assert skip_result["profile"] is None
    assert skip_result["posts"] == []

    sequence_result = asdict([fetched])
    assert isinstance(sequence_result, list)
    assert sequence_result[0] == keep_result

    assert dataclass_asdict(fetched) == keep_result

    @dataclass
    class Envelope:
        author: RuntimeAuthor

    assert dataclass_asdict(Envelope(fetched))["author"] == keep_result

    factory_result = dataclass_asdict(fetched, dict_factory=FactoryDict)
    assert type(factory_result) is FactoryDict
    assert type(factory_result["metadata"]) is dict

    partially_included = author_table.find_first(
        where={"id": author.id},
        include={"profile": True},
    )
    assert partially_included is not None
    partially_included_state = LAZY_RELATION_REGISTRY.get(partially_included)
    assert partially_included_state is not None
    assert set(partially_included_state) == {"posts"}
    partially_included_result = asdict(partially_included)
    assert partially_included_result["profile"]["bio"] == "hi"
    assert partially_included_result["posts"] == []

    included = author_table.find_first(
        where={"id": author.id},
        include={"profile": True, "posts": True},
    )
    assert included is not None
    assert LAZY_RELATION_REGISTRY.get(included) is None
    included_result = asdict(included)
    assert included_result["profile"]["bio"] == "hi"
    assert len(included_result["posts"]) == 2
    assert dataclass_asdict(included) == included_result
    included_skip_result = asdict(included, relation_policy="skip")
    assert included_skip_result["profile"] is None
    assert included_skip_result["posts"] == []

    post_table.insert(PostInsert(id=22, user_id=author.id, title="third"))
    with record_sql() as sqls:
        snapshot_result = asdict(included)
    assert sqls == []
    assert snapshot_result == included_result

    refreshed_result = asdict(fetched, relation_policy="fetch")
    assert len(refreshed_result["posts"]) == 3

    assigned = author_table.find_first(where={"id": author.id})
    assert assigned is not None
    assigned.posts = []
    assigned_state = LAZY_RELATION_REGISTRY.get(assigned)
    assert assigned_state is not None
    assert set(assigned_state) == {"profile"}
    assert assigned.posts == []

    client.__class__.close_all()
