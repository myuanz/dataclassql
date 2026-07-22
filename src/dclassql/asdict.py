from __future__ import annotations

import dataclasses as _dataclasses
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import fields, is_dataclass
from typing import Any, Literal, cast

from .runtime.backends.lazy import (
    LAZY_RELATION_REGISTRY,
    LazyInstance,
    LazyRelationState,
    _LazyRelationDescriptor,
)
from .runtime.backends.relation_view import LazyLookupKey

RelationPolicy = Literal['skip', 'fetch', 'keep']
type _DictFactory = Callable[[Iterable[tuple[str, Any]]], Any]
_SEQUENCE_SKIP_TYPES = (str, bytes, bytearray)


def asdict(value: Any, *, relation_policy: RelationPolicy = 'keep') -> Any:
    return _AsdictConverter(relation_policy, dict).convert(value)


class _AsdictConverter:
    def __init__(
        self,
        relation_policy: RelationPolicy,
        dict_factory: _DictFactory,
    ) -> None:
        self.relation_policy = relation_policy
        self.dict_factory = dict_factory
        self.memo: set[int] = set()
        self.relation_guard: set[LazyLookupKey] = set()

    def convert(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, LazyInstance):
            return self.convert(value._lazy_resolve())
        if is_dataclass(value):
            return self._convert_dataclass(value)
        if isinstance(value, Mapping):
            return {key: self.convert(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.convert(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self.convert(item) for item in value)
        if isinstance(value, Sequence) and not isinstance(
            value,
            _SEQUENCE_SKIP_TYPES,
        ):
            return [self.convert(item) for item in value]
        return value

    def _convert_dataclass(self, instance: Any) -> Any:
        instance_id = id(instance)
        if instance_id in self.memo:
            raise RecursionError(
                'dclassql.asdict() detected a recursive dataclass reference'
            )
        self.memo.add(instance_id)
        try:
            state_map = LAZY_RELATION_REGISTRY.get(instance)
            result: list[tuple[str, Any]] = []
            for field_obj in fields(instance):
                name = field_obj.name
                state = None if state_map is None else state_map.get(name)
                descriptor = _LazyRelationDescriptor.find(instance.__class__, name)
                if self.relation_policy == 'skip' and descriptor is not None:
                    value = [] if descriptor.many else None
                elif state is not None:
                    value = self._convert_relation(instance, state)
                else:
                    value = self.convert(getattr(instance, name))
                result.append((name, value))
            return self.dict_factory(result)
        finally:
            self.memo.remove(instance_id)

    def _convert_relation(
        self,
        owner: Any,
        state: LazyRelationState,
    ) -> Any:
        lookup_key = state.lookup_key_for(owner)
        guarded = lookup_key.criteria is not None
        if guarded and lookup_key in self.relation_guard:
            return [] if state.many else None

        if guarded:
            self.relation_guard.add(lookup_key)

        try:
            if self.relation_policy != 'fetch':
                return [] if state.many else None

            value = lookup_key.resolve()
            if value is None:
                return [] if state.many else None
            if state.many:
                return [
                    self.convert(item)
                    for item in value
                    if not is_dataclass(item) or id(item) not in self.memo
                ]
            if is_dataclass(value) and id(value) in self.memo:
                return None
            return self.convert(value)
        finally:
            if guarded:
                self.relation_guard.discard(lookup_key)


__all__ = ['RelationPolicy', 'asdict']


def _patch_dataclasses_asdict() -> None:
    original = getattr(_dataclasses, '_dclassql_original_asdict_inner', None)
    if original is not None:
        return

    original_inner = cast(Any, getattr(_dataclasses, '_asdict_inner'))

    def _patched_inner(obj: Any, dict_factory: Any):
        if obj in LAZY_RELATION_REGISTRY:
            return _AsdictConverter('keep', dict_factory).convert(obj)
        return original_inner(obj, dict_factory)

    setattr(_dataclasses, '_asdict_inner', _patched_inner)
    setattr(_dataclasses, '_dclassql_original_asdict_inner', original_inner)


_patch_dataclasses_asdict()
