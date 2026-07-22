from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, cast, runtime_checkable
from weakref import WeakKeyDictionary

from .protocols import BackendProtocol
from .relation_view import (
    LazyLookupKey,
    LazyRelationView,
    _LazyLookupProxy,
)


@dataclass(slots=True)
class LazyRelationState:
    attribute: str
    backend: BackendProtocol
    table_cls: type[Any]
    mapping: Mapping[str, str]
    many: bool
    materialized: bool = False
    value: Any = None
    model_cls: type[Any] | None = None

    def lookup_key_for(self, instance: Any) -> LazyLookupKey:
        criteria: list[tuple[str, object]] = []
        for local_column, remote_column in self.mapping.items():
            value = getattr(instance, local_column)
            if value is None:
                return LazyLookupKey(
                    self.backend,
                    self.table_cls,
                    None,
                    self.many,
                )
            criteria.append((remote_column, value))
        return LazyLookupKey(
            self.backend,
            self.table_cls,
            tuple(criteria),
            self.many,
        )


@dataclass(slots=True)
class _LazyRelationQuery:
    lookup_key: LazyLookupKey
    resolved: bool = False
    value: Any = None
    resolving: bool = False

    def resolve(self) -> Any:
        if self.resolved or self.resolving:
            return self.value
        self.resolving = True
        self.value = [] if self.lookup_key.many else None
        try:
            self.value = self.lookup_key.resolve()
            self.resolved = True
            return self.value
        finally:
            self.resolving = False


class _LazyRelationDescriptor:
    __slots__ = ("name", "original")

    def __init__(self, name: str, original: Any = None) -> None:
        self.name = name
        self.original = original

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self.name = name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        if instance is None:
            return self
        state_map = LAZY_RELATION_STATE.get(instance)
        if state_map is None:
            return self._get_original_value(instance)
        state = state_map.get(self.name)
        if state is None:
            return self._get_original_value(instance)
        if state.materialized:
            return state.value
        return ensure_lazy_placeholder(instance, state)

    def _get_original_value(self, instance: Any) -> Any:
        original = self.original
        if original is not None and hasattr(original, "__get__"):
            return original.__get__(instance, type(instance))
        if hasattr(instance, "__dict__"):
            return instance.__dict__.get(self.name)
        raise AttributeError(self.name)

    def _set_original_value(self, instance: Any, value: Any) -> None:
        original = self.original
        if original is not None and hasattr(original, "__set__"):
            original.__set__(instance, value)

    def __set__(self, instance: Any, value: Any) -> None:
        state_map = LAZY_RELATION_STATE.get(instance)
        if state_map is not None:
            state = state_map.get(self.name)
            if state is not None:
                state.materialized = True
                state.value = value
        self._set_original_value(instance, value)
        if hasattr(instance, "__dict__"):
            instance.__dict__[self.name] = value


_LAZY_SINGLE_PROXY_CLASS_CACHE: dict[type[Any], type[Any]] = {}
_LAZY_DESCRIPTOR_CACHE: dict[type[Any], dict[str, _LazyRelationDescriptor]] = {}
LAZY_RELATION_STATE: WeakKeyDictionary[Any, dict[str, LazyRelationState]] = WeakKeyDictionary()


ValueT = TypeVar("ValueT")


@runtime_checkable
class LazyInstance[ValueT](Protocol):
    __lazy_marker__: bool

    def _lazy_resolve(self) -> ValueT: ...


def eager[ValueT](value: LazyInstance[ValueT] | ValueT) -> ValueT:
    if isinstance(value, LazyRelationView):
        raise TypeError("eager() does not support LazyRelationView; use list(value)")
    if isinstance(value, LazyInstance):
        resolved = value._lazy_resolve()
        return cast(ValueT, resolved)
    return value


def ensure_lazy_descriptor(model_cls: type[Any], attribute: str) -> None:
    descriptor_map = _LAZY_DESCRIPTOR_CACHE.setdefault(model_cls, {})
    if attribute in descriptor_map:
        return
    if getattr(model_cls, "__hash__", None) is None:
        setattr(model_cls, "__hash__", object.__hash__)
    original = getattr(model_cls, attribute, None)
    descriptor = _LazyRelationDescriptor(attribute, original)
    descriptor_map[attribute] = descriptor
    setattr(model_cls, attribute, descriptor)


def _ensure_lazy_single_proxy_class(model_cls: type[Any]) -> type[Any]:
    cached = _LAZY_SINGLE_PROXY_CLASS_CACHE.get(model_cls)
    if cached is not None:
        return cached

    field_names = frozenset(getattr(model_cls, "__dataclass_fields__", ()))

    def __init__(self: Any, lookup_key: LazyLookupKey) -> None:
        object.__setattr__(self, "_lazy_query", _LazyRelationQuery(lookup_key))

    def _lazy_resolve(self: Any) -> Any:
        query = cast(_LazyRelationQuery, object.__getattribute__(self, "_lazy_query"))
        return query.resolve()

    def __repr__(self: Any) -> str:
        query = cast(_LazyRelationQuery, object.__getattribute__(self, "_lazy_query"))
        if not query.resolved:
            return f"<LazyRelation {model_cls.__name__} (lazy)>"
        return repr(query.value)

    def __str__(self: Any) -> str:
        query = cast(_LazyRelationQuery, object.__getattribute__(self, "_lazy_query"))
        if not query.resolved:
            return f"<LazyRelation {model_cls.__name__} (lazy)>"
        return str(query.value)

    def __bool__(self: Any) -> bool:
        return bool(_lazy_resolve(self))

    def __setattr__(self: Any, name: str, value: Any) -> None:
        if name == "_lazy_query":
            object.__setattr__(self, name, value)
            return
        target = _lazy_resolve(self)
        setattr(target, name, value)

    def __delattr__(self: Any, name: str) -> None:
        if name == "_lazy_query":
            raise AttributeError(name)
        target = _lazy_resolve(self)
        delattr(target, name)

    def __getattribute__(self: Any, name: str) -> Any:
        if name == "_lazy_query":
            return object.__getattribute__(self, name)
        if name in field_names:
            return getattr(_lazy_resolve(self), name)
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return getattr(_lazy_resolve(self), name)

    namespace: dict[str, Any] = {
        "__slots__": ("_lazy_query",),
        "__init__": __init__,
        "_lazy_resolve": _lazy_resolve,
        "__repr__": __repr__,
        "__str__": __str__,
        "__bool__": __bool__,
        "__eq__": _LazyLookupProxy.__eq__,
        "__ne__": _LazyLookupProxy.__ne__,
        "__hash__": _LazyLookupProxy.__hash__,
        "__setattr__": __setattr__,
        "__delattr__": __delattr__,
        "__getattribute__": __getattribute__,
        "__lazy_marker__": True,
    }

    proxy_cls = type(
        f"{model_cls.__name__}LazyRelationProxy",
        (model_cls, _LazyLookupProxy),
        namespace,
    )
    proxy_cls.__module__ = model_cls.__module__
    _LAZY_SINGLE_PROXY_CLASS_CACHE[model_cls] = proxy_cls
    return proxy_cls


def _create_lazy_single_proxy(owner: Any, state: LazyRelationState) -> Any:
    model_cls = state.model_cls
    if model_cls is None:
        model_cls = cast(type[Any], getattr(state.table_cls, "model", None))
        if model_cls is None:
            raise RuntimeError(f"Relation '{state.attribute}' missing model class metadata")
        state.model_cls = model_cls
    proxy_cls = _ensure_lazy_single_proxy_class(model_cls)
    return proxy_cls(state.lookup_key_for(owner))


def ensure_lazy_placeholder(instance: Any, state: LazyRelationState) -> Any:
    if state.many:
        return LazyRelationView(
            state.attribute,
            state.lookup_key_for(instance),
        )
    return _create_lazy_single_proxy(instance, state)


def ensure_lazy_state(
    instance: Any,
    attribute: str,
    backend: BackendProtocol,
    table_cls: type[Any],
    mapping: Mapping[str, str],
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
        state.value = state.lookup_key_for(instance).resolve()
        state.materialized = True
    else:
        state.materialized = False
        state.value = None
        if hasattr(instance, "__dict__"):
            instance.__dict__.pop(state.attribute, None)
