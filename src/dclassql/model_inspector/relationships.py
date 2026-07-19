from collections import defaultdict
from dataclasses import dataclass, fields
from types import CellType, FunctionType
from typing import Any, Iterable, Mapping, Self, Sequence

from .fields import FieldTo
from .table_constraints import Col
from .type_hints import FieldToTypeHint


@dataclass(slots=True, frozen=True)
class Link:
    source: type[Any]
    attribute: str
    target: type[Any]
    many: bool

    def __repr__(self) -> str:
        target = repr(self.target)
        target = f"list[{target}]" if self.many else target
        return f"Link({self.source!r}.{self.attribute} -> {target})"


type FieldToLink = FieldTo[Link]


@dataclass(slots=True, frozen=True)
class LocalRemotePair[T]:
    local: T
    remote: T

    def reversed(self) -> 'LocalRemotePair[T]':
        return LocalRemotePair(local=self.remote, remote=self.local)


@dataclass(slots=True, frozen=True)
class Relationship:
    local: Link
    remote: Link | None
    mapping: Mapping[Col, Col]

    def reversed(self) -> 'Relationship':
        if self.remote is None:
            raise ValueError("One-way relationship cannot be reversed")
        return Relationship(
            local=self.remote,
            remote=self.local,
            mapping={remote: local for local, remote in self.mapping.items()},
        )

    def __repr__(self) -> str:
        mapping = "、".join(
            f"`{self.local.source.__name__}.{local.name} == "
            f"{self.local.target.__name__}.{remote.name}`"
            for local, remote in self.mapping.items()
        )
        result = (
            f"Relationship(通过{mapping}将"
            f"`{self.local.source.__name__}.{self.local.attribute}` "
            f"连接到 `{self.local.target.__name__}`"
        )
        if self.remote is not None:
            remote_target = self.remote.target.__name__
            if self.remote.many:
                remote_target = f"list[{remote_target}]"
            result += (
                f"；在 `{self.remote.source.__name__}` 中可通过 "
                f"`{self.remote.source.__name__}.{self.remote.attribute}` "
                f"访问到 `{remote_target}`"
            )
        return result + ")"


class Relationships:
    def __init__(self, relationships: Sequence[Relationship]) -> None:
        by_local_model: defaultdict[type[Any], list[Relationship]] = defaultdict(list)
        by_remote_model: defaultdict[type[Any], list[Relationship]] = defaultdict(list)
        by_model: defaultdict[type[Any], list[Relationship]] = defaultdict(list)
        for relationship in relationships:
            by_local_model[relationship.local.source].append(relationship)
            by_remote_model[relationship.local.target].append(relationship)
            by_model[relationship.local.source].append(relationship)
            if relationship.remote is not None:
                by_model[relationship.remote.source].append(relationship.reversed())
        self._by_local_model = {
            model: tuple(items) for model, items in by_local_model.items()
        }
        self._by_remote_model = {
            model: tuple(items) for model, items in by_remote_model.items()
        }
        self._by_model = {model: tuple(items) for model, items in by_model.items()}

    def by_local_model(self, model: type[Any]) -> tuple[Relationship, ...]:
        return self._by_local_model.get(model, ())

    def by_remote_model(self, model: type[Any]) -> tuple[Relationship, ...]:
        return self._by_remote_model.get(model, ())

    def by_model(self, model: type[Any]) -> tuple[Relationship, ...]:
        return self._by_model.get(model, ())


@dataclass(slots=True, frozen=True)
class _ForeignKeyComparison:
    left: '_ProxyCol'
    right: '_ProxyCol'


@dataclass(slots=True, frozen=True, eq=False)
class _ProxyCol(Col):
    model: type[Any]
    link: '_ProxyLink | None' = None

    def __eq__(self, other: object) -> _ForeignKeyComparison | bool:  # type: ignore[override]
        if not isinstance(other, _ProxyCol):
            return NotImplemented  # type: ignore[return-value]
        return _ForeignKeyComparison(self, other)

    def _to_base(self) -> Col:
        return Col(self.name)


@dataclass(slots=True, frozen=True)
class _ProxyLink(Link):
    @classmethod
    def from_link(cls, link: Link) -> Self:
        return cls(
            source=link.source,
            attribute=link.attribute,
            target=link.target,
            many=link.many,
        )

    def to_link(self) -> Link:
        return Link(
            source=self.source,
            attribute=self.attribute,
            target=self.target,
            many=self.many,
        )

    def __getattr__(self, name: str) -> _ProxyCol:
        return _ProxyCol(name, model=self.target, link=self)


class _FakeSelf:
    def __init__(
        self,
        model: type[Any],
        type_hints: FieldToTypeHint,
        links: FieldToLink,
    ) -> None:
        self._model = model
        self._type_hints = type_hints
        self._links = links

    def __getattr__(self, name: str) -> _ProxyCol | _ProxyLink:
        if name not in self._type_hints:
            raise AttributeError(name)
        link = self._links.get(name)
        if link is not None:
            return _ProxyLink.from_link(link)
        return _ProxyCol(name, model=self._model)


class _ProxyModel:
    def __init__(
        self,
        model: type[Any],
        type_hints: FieldToTypeHint,
        links: FieldToLink,
    ) -> None:
        self._model = model
        self._type_hints = type_hints
        self._links = links

    def __getattr__(self, name: str) -> _ProxyCol | Link:
        if name not in self._type_hints:
            raise AttributeError(name)
        link = self._links.get(name)
        if link is not None:
            return link
        return _ProxyCol(name, model=self._model)


def inspect_relationships(
    models: Sequence[type[Any]],
    type_hints_by_model: Mapping[type[Any], FieldToTypeHint],
    registry: Mapping[str, type[Any]],
) -> Relationships:
    # 注解只形成 Link；foreign_key() 才会将其确认为 Relationship。
    links_by_model = {
        model: _inspect_links(model, type_hints_by_model[model], registry)
        for model in models
    }
    model_proxies = {
        model: _ProxyModel(
            model,
            type_hints_by_model[model],
            links_by_model[model],
        )
        for model in models
    }
    relationships: list[Relationship] = []
    for model in models:
        relationships.extend(
            _inspect_model_relationships(
                model,
                type_hints_by_model[model],
                links_by_model[model],
                model_proxies,
            )
        )
    return Relationships(relationships)


def _inspect_links(
    model: type[Any],
    type_hints: FieldToTypeHint,
    registry: Mapping[str, type[Any]],
) -> FieldToLink:
    links: dict[str, Link] = {}
    for field in fields(model):
        name = field.name
        type_hint = type_hints.get(name)
        if type_hint is None:
            continue
        if isinstance(type_hint.value, type) and type_hint.value in registry.values():
            links[name] = Link(
                source=model,
                attribute=name,
                target=type_hint.value,
                many=type_hint.is_collection,
            )
    return FieldTo.from_mapping(links)


def _inspect_model_relationships(
    model: type[Any],
    type_hints: FieldToTypeHint,
    links: FieldToLink,
    model_proxies: Mapping[type[Any], object],
) -> list[Relationship]:
    if not hasattr(model, "foreign_key"):
        return []
    fake = _FakeSelf(model, type_hints, links)
    fn = getattr(model, "foreign_key")
    if not isinstance(fn, FunctionType):
        raise TypeError("foreign_key must be an instance method")
    rebound = _rebind_model_references(fn, model_proxies)
    entries = _iterate_results(rebound(fake))
    relationships: list[Relationship] = []
    for entry in entries:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise TypeError("foreign_key must yield tuples of (comparison, backref)")
        comparison, backref = entry
        if not isinstance(comparison, _ForeignKeyComparison):
            raise TypeError("foreign_key comparison must be column equality")
        local_col, remote_col = _determine_direction(model, comparison)
        remote_model = remote_col.model
        proxy_link = remote_col.link
        if proxy_link is None:
            raise TypeError("foreign_key comparison must use a Link on the local model")
        local_link = proxy_link.to_link()
        if backref is not None and not isinstance(backref, Link):
            raise TypeError(
                f"foreign_key backref must be a relation attribute or None, {backref!r} given"
            )
        remote_link = backref
        if remote_link is not None and (
            remote_link.source is not remote_model or remote_link.target is not model
        ):
            raise TypeError(
                f"Relationship backref {remote_link.source.__name__}."
                f"{remote_link.attribute} must connect {remote_model.__name__} "
                f"to {model.__name__}"
            )
        relationships.append(
            Relationship(
                local=local_link,
                remote=remote_link,
                mapping={
                    local_col._to_base(): remote_col._to_base(),
                },
            )
        )
    return relationships


def _rebind_model_references(
    fn: FunctionType,
    model_proxies: Mapping[type[Any], object],
) -> FunctionType:
    # 替换模型引用，让 foreign_key() 在不修改原始 class 的环境中执行。
    globals_ = dict(fn.__globals__)
    for name, value in fn.__globals__.items():
        if isinstance(value, type) and value in model_proxies:
            globals_[name] = model_proxies[value]
    closure = (
        tuple(
            CellType(model_proxies[value])
            if isinstance((value := cell.cell_contents), type) and value in model_proxies
            else cell
            for cell in fn.__closure__
        )
        if fn.__closure__ is not None
        else None
    )
    rebound = FunctionType(
        fn.__code__,
        globals_,
        fn.__name__,
        fn.__defaults__,
        closure,
    )
    rebound.__kwdefaults__ = fn.__kwdefaults__
    return rebound


def _iterate_results(results: Any) -> Iterable[Any]:
    if results is None:
        return []
    if isinstance(results, Iterable) and not isinstance(results, (str, bytes)):
        return results
    return [results]


def _determine_direction(
    model: type[Any],
    comparison: _ForeignKeyComparison,
) -> tuple[_ProxyCol, _ProxyCol]:
    left_local = comparison.left.model is model
    right_local = comparison.right.model is model
    if left_local and not right_local:
        return comparison.left, comparison.right
    if right_local and not left_local:
        return comparison.right, comparison.left
    raise ValueError("Unable to determine foreign key direction")
