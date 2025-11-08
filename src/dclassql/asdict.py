from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from typing import Any, Literal

from .runtime.backends.lazy import (
    LAZY_RELATION_STATE,
    LazyInstance,
    LazyRelationState,
    resolve_lazy_relation,
    _LazyProxyBase,
)

RelationPolicy = Literal['skip', 'fetch', 'keep']
RelationKey = tuple[type[Any] | None, tuple[tuple[str, Any], ...]]
_SEQUENCE_SKIP_TYPES = (str, bytes, bytearray)


def asdict(value: Any, *, relation_policy: RelationPolicy = 'keep') -> Any:
    if value is None:
        return None

    memo: set[int] = set()
    relation_guard: set[RelationKey] = set()
    return _convert_value(value, relation_policy, memo, relation_guard)


def _convert_value(
    value: Any,
    relation_policy: RelationPolicy,
    memo: set[int],
    relation_guard: set[RelationKey],
) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return _convert_dataclass(value, relation_policy, memo, relation_guard)
    if isinstance(value, LazyInstance):
        resolved = value._lazy_resolve()
        return _convert_value(resolved, relation_policy, memo, relation_guard)
    if isinstance(value, Mapping):
        return {
            key: _convert_value(item, relation_policy, memo, relation_guard)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_convert_value(item, relation_policy, memo, relation_guard) for item in value]
    if isinstance(value, tuple):
        return tuple(_convert_value(item, relation_policy, memo, relation_guard) for item in value)
    if isinstance(value, Sequence) and not isinstance(value, _SEQUENCE_SKIP_TYPES):
        return [_convert_value(item, relation_policy, memo, relation_guard) for item in value]
    return value


def _convert_dataclass(
    instance: Any,
    relation_policy: RelationPolicy,
    memo: set[int],
    relation_guard: set[RelationKey],
) -> dict[str, Any]:
    instance_id = id(instance)
    if instance_id in memo:
        raise RecursionError('dclassql.asdict() detected a recursive dataclass reference')
    memo.add(instance_id)
    try:
        state_map = LAZY_RELATION_STATE.get(instance)
        result: dict[str, Any] = {}
        for field_obj in fields(instance):
            name = field_obj.name
            state = _lookup_relation_state(state_map, name)
            if state is not None:
                result[name] = _convert_relation(instance, state, relation_policy, memo, relation_guard)
                continue
            value = getattr(instance, name)
            result[name] = _convert_value(value, relation_policy, memo, relation_guard)
        return result
    finally:
        memo.remove(instance_id)


def _lookup_relation_state(state_map: dict[str, LazyRelationState] | None, name: str) -> LazyRelationState | None:
    if state_map is None:
        return None
    return state_map.get(name)


def _convert_relation(
    owner: Any,
    state: LazyRelationState,
    relation_policy: RelationPolicy,
    memo: set[int],
    relation_guard: set[RelationKey],
) -> Any:
    relation_key = _relation_identity(owner, state)
    if relation_key is not None and relation_key in relation_guard:
        return [] if state.many else None

    guard_added = False
    if relation_key is not None:
        relation_guard.add(relation_key)
        guard_added = True

    try:
        if relation_policy == 'skip':
            return [] if state.many else None

        if relation_policy == 'fetch':
            value = resolve_lazy_relation(owner, state)
        elif state.loaded and not isinstance(state.value, _LazyProxyBase):
            value = state.value
        else:
            if relation_policy == 'keep':
                return [] if state.many else None
            value = resolve_lazy_relation(owner, state)

        if isinstance(value, _LazyProxyBase):
            if state.many:
                value = list(value)
            else:
                value = value._lazy_resolve()

        if value is None:
            return None if not state.many else []

        if state.many:
            result_list: list[Any] = []
            for item in value:
                if is_dataclass(item) and id(item) in memo:
                    continue
                result_list.append(_convert_value(item, relation_policy, memo, relation_guard))
            return result_list

        if is_dataclass(value) and id(value) in memo:
            return None

        return _convert_value(value, relation_policy, memo, relation_guard)
    finally:
        if guard_added:
            relation_guard.discard(relation_key)


def _relation_identity(owner: Any, state: LazyRelationState) -> RelationKey | None:
    mapping = state.mapping
    if not mapping:
        return None
    values: list[tuple[str, Any]] = []
    for owner_column, target_column in mapping:
        owner_value = getattr(owner, owner_column, None)
        if owner_value is None:
            return None
        values.append((target_column, owner_value))
    model_cls = getattr(state.table_cls, 'model', None)
    return (model_cls, tuple(values))


__all__ = ['RelationPolicy', 'asdict']
