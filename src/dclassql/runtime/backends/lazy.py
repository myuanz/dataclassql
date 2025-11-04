from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, SupportsIndex, cast
from weakref import WeakKeyDictionary

from .protocols import BackendProtocol


@dataclass(slots=True)
class LazyRelationState:
    attribute: str
    backend: BackendProtocol[Any, Any, Mapping[str, object]]
    table_cls: type[Any]
    mapping: tuple[tuple[str, str], ...]
    many: bool
    loaded: bool = False
    value: Any = None
    loading: bool = False
    model_cls: type[Any] | None = None


class _LazyRelationDescriptor:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self.name = name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        if instance is None:
            return self
        state_map = LAZY_RELATION_STATE.get(instance)
        if state_map is None:
            return instance.__dict__.get(self.name)
        state = state_map.get(self.name)
        if state is None:
            return instance.__dict__.get(self.name)
        if state.loaded:
            return state.value
        value = ensure_lazy_placeholder(instance, state)
        if hasattr(instance, "__dict__"):
            instance.__dict__[self.name] = value
        return value

    def __set__(self, instance: Any, value: Any) -> None:
        state_map = LAZY_RELATION_STATE.get(instance)
        if state_map is not None:
            state = state_map.get(self.name)
            if state is not None:
                state.loaded = True
                state.value = value
                state.loading = False
        if hasattr(instance, "__dict__"):
            instance.__dict__[self.name] = value


class _LazyProxyBase:
    __slots__ = ()


class _LazyListProxy(_LazyProxyBase, list[Any]):
    __slots__ = ("_lazy_owner", "_lazy_state")

    def __init__(self, owner: Any, state: LazyRelationState) -> None:
        super().__init__()
        object.__setattr__(self, "_lazy_owner", owner)
        object.__setattr__(self, "_lazy_state", state)

    def _lazy_resolve(self) -> list[Any]:
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        owner = object.__getattribute__(self, "_lazy_owner")
        return cast(list[Any], resolve_lazy_relation(owner, state))

    def __repr__(self) -> str:
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        if not state.loaded:
            return f"<LazyRelationList {state.attribute} (lazy)>"
        return repr(state.value)

    def __str__(self) -> str:
        return self.__repr__()

    def __bool__(self) -> bool:
        return bool(self._lazy_resolve())

    def __len__(self) -> int:
        return len(self._lazy_resolve())

    def __iter__(self) -> Iterator[Any]:
        return iter(self._lazy_resolve())

    def __getitem__(self, index: SupportsIndex | slice) -> Any:
        return self._lazy_resolve()[index]

    def __setitem__(self, index: SupportsIndex | slice, value: Any) -> None:
        self._lazy_resolve()[index] = value

    def append(self, value: Any) -> None:
        self._lazy_resolve().append(value)

    def extend(self, values: Iterable[Any]) -> None:
        self._lazy_resolve().extend(values)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._lazy_resolve(), name)


_LAZY_SINGLE_PROXY_CLASS_CACHE: dict[type[Any], type[Any]] = {}
_LAZY_DESCRIPTOR_CACHE: dict[type[Any], dict[str, _LazyRelationDescriptor]] = {}
LAZY_RELATION_STATE: WeakKeyDictionary[Any, dict[str, LazyRelationState]] = WeakKeyDictionary()


def ensure_lazy_descriptor(model_cls: type[Any], attribute: str) -> None:
    descriptor_map = _LAZY_DESCRIPTOR_CACHE.setdefault(model_cls, {})
    if attribute in descriptor_map:
        return
    if getattr(model_cls, "__hash__", None) is None:
        setattr(model_cls, "__hash__", object.__hash__)
    descriptor = _LazyRelationDescriptor(attribute)
    descriptor_map[attribute] = descriptor
    setattr(model_cls, attribute, descriptor)


def _ensure_lazy_single_proxy_class(model_cls: type[Any]) -> type[Any]:
    cached = _LAZY_SINGLE_PROXY_CLASS_CACHE.get(model_cls)
    if cached is not None:
        return cached

    def __init__(self: Any, owner: Any, state: LazyRelationState) -> None:
        object.__setattr__(self, "_lazy_owner", owner)
        object.__setattr__(self, "_lazy_state", state)

    def _lazy_resolve(self: Any) -> Any:
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        owner = object.__getattribute__(self, "_lazy_owner")
        return resolve_lazy_relation(owner, state)

    def __repr__(self: Any) -> str:
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        if not state.loaded:
            return f"<LazyRelation {model_cls.__name__} (lazy)>"
        return repr(state.value)

    def __str__(self: Any) -> str:
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        if not state.loaded:
            return f"<LazyRelation {model_cls.__name__} (lazy)>"
        return str(state.value)

    def __bool__(self: Any) -> bool:
        return bool(_lazy_resolve(self))

    def __eq__(self: Any, other: object) -> bool:
        return _lazy_resolve(self) == other

    def __hash__(self: Any) -> int:
        return hash(_lazy_resolve(self))

    def __setattr__(self: Any, name: str, value: Any) -> None:
        if name in {"_lazy_owner", "_lazy_state"}:
            object.__setattr__(self, name, value)
            return
        target = _lazy_resolve(self)
        setattr(target, name, value)
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        state.value = target
        state.loaded = True

    def __delattr__(self: Any, name: str) -> None:
        if name in {"_lazy_owner", "_lazy_state"}:
            raise AttributeError(name)
        target = _lazy_resolve(self)
        delattr(target, name)

    def __getattribute__(self: Any, name: str) -> Any:
        if name in {"_lazy_owner", "_lazy_state"}:
            return object.__getattribute__(self, name)
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            pass
        state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
        if state.loaded:
            value = state.value
        else:
            owner = object.__getattribute__(self, "_lazy_owner")
            value = resolve_lazy_relation(owner, state)
        return getattr(value, name)

    namespace: dict[str, Any] = {
        "__slots__": ("_lazy_owner", "_lazy_state"),
        "__init__": __init__,
        "_lazy_resolve": _lazy_resolve,
        "__repr__": __repr__,
        "__str__": __str__,
        "__bool__": __bool__,
        "__eq__": __eq__,
        "__hash__": __hash__,
        "__setattr__": __setattr__,
        "__delattr__": __delattr__,
        "__getattribute__": __getattribute__,
        "__lazy_marker__": True,
    }

    proxy_cls = type(f"{model_cls.__name__}LazyRelationProxy", (model_cls, _LazyProxyBase), namespace)
    proxy_cls.__module__ = model_cls.__module__
    _LAZY_SINGLE_PROXY_CLASS_CACHE[model_cls] = proxy_cls
    return proxy_cls


def _lazy_resolve(self: Any) -> Any:
    state = cast(LazyRelationState, object.__getattribute__(self, "_lazy_state"))
    owner = object.__getattribute__(self, "_lazy_owner")
    return resolve_lazy_relation(owner, state)


def resolve_lazy_relation(instance: Any, state: LazyRelationState) -> Any:
    if state.loaded:
        return state.value
    if state.loading:
        return state.value
    state.loading = True
    state.value = [] if state.many else None
    where: dict[str, object] = {}
    for owner_column, target_column in state.mapping:
        owner_value = getattr(instance, owner_column, None)
        if owner_value is None:
            value: Any = [] if state.many else None
            state.loaded = True
            state.value = value
            state.loading = False
            if hasattr(instance, "__dict__"):
                instance.__dict__[state.attribute] = value
            return value
        where[target_column] = owner_value

    table = state.table_cls(state.backend)
    if state.many:
        loaded = table.find_many(where=cast(Mapping[str, object], where))
    else:
        loaded = table.find_first(where=cast(Mapping[str, object], where))

    if state.many and loaded is None:
        loaded = []

    state.loaded = True
    state.value = loaded
    state.loading = False
    if hasattr(instance, "__dict__"):
        instance.__dict__[state.attribute] = loaded
    return loaded


def _create_lazy_single_proxy(owner: Any, state: LazyRelationState) -> Any:
    model_cls = state.model_cls
    if model_cls is None:
        model_cls = cast(type[Any], getattr(state.table_cls, "model", None))
        if model_cls is None:
            raise RuntimeError(f"Relation '{state.attribute}' missing model class metadata")
        state.model_cls = model_cls
    proxy_cls = _ensure_lazy_single_proxy_class(model_cls)
    return proxy_cls(owner, state)


def ensure_lazy_placeholder(instance: Any, state: LazyRelationState) -> Any:
    if state.loaded:
        return state.value
    existing = state.value
    if isinstance(existing, _LazyProxyBase):
        return existing
    if state.loading:
        return existing
    if state.many:
        proxy = _LazyListProxy(instance, state)
    else:
        proxy = _create_lazy_single_proxy(instance, state)
    state.value = proxy
    state.loading = False
    if hasattr(instance, "__dict__"):
        instance.__dict__[state.attribute] = proxy
    return proxy


def ensure_lazy_state(
    instance: Any,
    attribute: str,
    backend: BackendProtocol[Any, Any, Mapping[str, object]],
    table_cls: type[Any],
    mapping: tuple[tuple[str, str], ...],
    many: bool,
) -> LazyRelationState:
    model_cls = instance.__class__
    ensure_lazy_descriptor(model_cls, attribute)
    state_map = LAZY_RELATION_STATE.get(instance)
    if state_map is None:
        state_map = {}
        LAZY_RELATION_STATE[instance] = state_map
    state = state_map.get(attribute)
    if state is None:
        state = LazyRelationState(
            attribute=attribute,
            backend=backend,
            table_cls=table_cls,
            mapping=mapping,
            many=many,
            model_cls=cast(type[Any] | None, getattr(table_cls, "model", None)),
        )
        state_map[attribute] = state
    else:
        state.backend = backend
        state.table_cls = table_cls
        state.mapping = mapping
        state.many = many
        state.model_cls = cast(type[Any] | None, getattr(table_cls, "model", None))
    return state


def finalize_lazy_state(instance: Any, state: LazyRelationState, eager: bool) -> None:
    if eager:
        resolve_lazy_relation(instance, state)
    else:
        state.loaded = False
        state.loading = False
        if not isinstance(state.value, _LazyProxyBase):
            state.value = None
        if hasattr(instance, "__dict__"):
            instance.__dict__.pop(state.attribute, None)


def reset_lazy_backref(owner: Any, attribute: str) -> None:
    state_map = LAZY_RELATION_STATE.get(owner)
    if state_map is None:
        return
    state = state_map.get(attribute)
    if state is None:
        return
    state.loaded = False
    state.loading = False
    state.value = [] if state.many else None
    if hasattr(owner, "__dict__"):
        owner.__dict__.pop(attribute, None)
