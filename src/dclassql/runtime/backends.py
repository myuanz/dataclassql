from __future__ import annotations

import sqlite3
import sys
import threading
from collections.abc import Iterator
from dataclasses import MISSING, dataclass, fields, is_dataclass
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Protocol,
    Sequence,
    SupportsIndex,
    cast,
    runtime_checkable,
    get_origin,
)

from weakref import WeakKeyDictionary

from pypika import Query, Table
from pypika.enums import Order
from pypika.terms import Parameter


ConnectionFactory = Callable[[], sqlite3.Connection]


@dataclass(slots=True)
class RelationSpec:
    name: str
    table_name: str
    table_module: str
    many: bool
    mapping: tuple[tuple[str, str], ...]


@runtime_checkable
class TableProtocol[ModelT, InsertT, WhereT](Protocol):
    def __init__(self, backend: BackendProtocol[ModelT, InsertT, WhereT]) -> None: ...

    model: type[ModelT]
    insert_model: type[InsertT]
    columns: tuple[str, ...]
    auto_increment_columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    relations: tuple[RelationSpec, ...]


@runtime_checkable
class BackendProtocol[ModelT, InsertT, WhereT](Protocol):
    def insert(self, table: TableProtocol[ModelT, InsertT, WhereT], data: InsertT | Mapping[str, object]) -> ModelT: ...

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]: ...

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]: ...

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        skip: int | None = None,
    ) -> ModelT | None: ...


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
        return cast(list[Any], _resolve_lazy_relation(owner, state))

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
        return _resolve_lazy_relation(owner, state)

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
        value = _lazy_resolve(self)
        return bool(value)

    def __eq__(self: Any, other: object) -> bool:
        value = _lazy_resolve(self)
        return value == other

    def __hash__(self: Any) -> int:
        value = _lazy_resolve(self)
        return hash(value)

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
            value = _resolve_lazy_relation(owner, state)
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
_LAZY_CLASS_CACHE: dict[type[Any], type[Any]] = {}
_LAZY_RELATION_STATE: WeakKeyDictionary[Any, dict[str, LazyRelationState]] = WeakKeyDictionary()


def _resolve_lazy_relation(instance: Any, state: LazyRelationState) -> Any:
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
            object.__setattr__(instance, state.attribute, value)
            state.loading = False
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
    object.__setattr__(instance, state.attribute, loaded)
    return loaded


def _create_lazy_single_proxy(owner: Any, state: LazyRelationState) -> Any:
    model_cls = state.model_cls
    if model_cls is None:
        model_cls = cast(type[Any], getattr(state.table_cls, "model", None))
        if model_cls is None:
            raise RuntimeError(f"Relation '{state.attribute}' missing model class metadata")
        state.model_cls = model_cls
    lazy_model_cls = _ensure_lazy_model_class(model_cls)
    proxy_cls = _ensure_lazy_single_proxy_class(lazy_model_cls)
    return proxy_cls(owner, state)


def _ensure_lazy_placeholder(instance: Any, state: LazyRelationState) -> Any:
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
    return proxy


def _ensure_lazy_model_class(model_cls: type[Any]) -> type[Any]:
    cached = _LAZY_CLASS_CACHE.get(model_cls)
    if cached is not None:
        return cached

    base_getattribute = cast(Callable[[Any, str], Any], model_cls.__getattribute__)
    base_setattr = cast(Callable[[Any, str, Any], None], model_cls.__setattr__)

    def __getattribute__(self: Any, name: str) -> Any:
        state_map = _LAZY_RELATION_STATE.get(self)
        if state_map is not None:
            state = state_map.get(name)
            if state is not None:
                if state.loaded:
                    return state.value
                placeholder = _ensure_lazy_placeholder(self, state)
                return placeholder
        return base_getattribute(self, name)

    def __setattr__(self: Any, name: str, value: Any) -> None:
        state_map = _LAZY_RELATION_STATE.get(self)
        if state_map is not None:
            state = state_map.get(name)
            if state is not None:
                state.loaded = True
                state.value = value
        base_setattr(self, name, value)

    namespace: dict[str, Any] = {
        "__slots__": (),
        "__lazy_base__": model_cls,
        "__getattribute__": __getattribute__,
        "__setattr__": __setattr__,
        "__hash__": object.__hash__,
    }
    lazy_cls = type(f"{model_cls.__name__}LazyRuntime", (model_cls,), namespace)
    try:
        lazy_cls.__name__ = model_cls.__name__
    except AttributeError:
        pass
    try:
        lazy_cls.__qualname__ = model_cls.__qualname__
    except AttributeError:
        pass
    lazy_cls.__module__ = model_cls.__module__
    _LAZY_CLASS_CACHE[model_cls] = lazy_cls
    return lazy_cls


def _find_backref_relations(
    table: TableProtocol[Any, Any, Any],
    existing_names: set[str],
) -> list[RelationSpec]:
    model = table.model
    module_name = table.__class__.__module__
    module = sys.modules.get(module_name)
    if module is None:
        return []

    extras: list[RelationSpec] = []
    for attr in dir(module):
        candidate = getattr(module, attr, None)
        if not isinstance(candidate, type):
            continue
        if candidate is table.__class__:
            continue
        foreign_keys = getattr(candidate, "foreign_keys", None)
        if not foreign_keys:
            continue
        candidate_model = getattr(candidate, "model", None)
        if candidate_model is None:
            continue
        for fk in foreign_keys:
            if fk.backref is None:
                continue
            if fk.remote_model is not model:
                continue
            name = fk.backref
            if name in existing_names:
                continue
            mapping_pairs: tuple[tuple[str, str], ...] = tuple(
                (remote, local) for remote, local in zip(fk.remote_columns, fk.local_columns)
            )
            primary_key = getattr(candidate, "primary_key", ())
            many = tuple(primary_key) != tuple(fk.local_columns)
            extras.append(
                RelationSpec(
                    name=name,
                    table_name=candidate.__name__,
                    table_module=candidate.__module__,
                    many=many,
                    mapping=mapping_pairs,
                )
            )
            existing_names.add(name)
    return extras


class SQLiteBackend[ModelT, InsertT, WhereT: Mapping[str, object]](BackendProtocol[ModelT, InsertT, WhereT]):
    def __init__(self, source: sqlite3.Connection | ConnectionFactory | "SQLiteBackend") -> None:
        if isinstance(source, SQLiteBackend):
            self._factory: ConnectionFactory | None = source._factory
            self._connection: sqlite3.Connection | None = source._connection
            self._local = source._local
            self._identity_map = source._identity_map
        elif isinstance(source, sqlite3.Connection):
            self._factory = None
            self._connection = source
            self._ensure_row_factory(self._connection)
            self._local = threading.local()
            self._identity_map: dict[tuple[type[Any], tuple[Any, ...]], Any] = {}
        elif callable(source):
            self._factory = source
            self._connection = None
            self._local = threading.local()
            self._identity_map = {}
        else:
            raise TypeError("SQLite backend source must be connection or callable returning connection")

    def insert(self, table: TableProtocol[ModelT, InsertT, WhereT], data: InsertT | Mapping[str, object]) -> ModelT:
        payload = self._normalize_insert_payload(table, data)
        if not payload:
            raise ValueError("Insert payload cannot be empty")

        table_name = table.model.__name__
        sql_table = Table(table_name)
        columns = list(payload.keys())
        params = [payload[name] for name in columns]

        insert_query = (
            Query.into(sql_table)
            .columns(*columns)
            .insert(*(Parameter("?") for _ in columns))
        )
        sql = insert_query.get_sql(quote_char='"') + ";"
        cursor = self._execute(sql, params)

        pk_filter: dict[str, object] = {}
        auto_increment = set(getattr(table, "auto_increment_columns", ()))
        primary_key = getattr(table, "primary_key", ())
        if not primary_key:
            raise ValueError(f"Table {table_name} does not define primary key")
        if len(primary_key) == 1:
            pk = primary_key[0]
            value = payload.get(pk)
            if value is None and pk in auto_increment:
                value = cursor.lastrowid
            if value is None:
                raise ValueError(f"Primary key column '{pk}' is null after insert")
            pk_filter[pk] = value
        else:
            for pk in primary_key:
                value = payload.get(pk)
                if value is None:
                    raise ValueError(f"Composite key requires '{pk}' value")
                pk_filter[pk] = value

        result = self._fetch_single(table, pk_filter, include=None)
        self._invalidate_backrefs(table, result)
        return result

    def insert_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: Sequence[InsertT | Mapping[str, object]],
        *,
        batch_size: int | None = None,
    ) -> list[ModelT]:
        items = list(data)
        if not items:
            return []

        payloads = [self._normalize_insert_payload(table, item) for item in items]
        columns = list(payloads[0].keys())
        if not columns:
            raise ValueError("Insert payload cannot be empty")

        params_matrix = [[payload.get(column) for column in columns] for payload in payloads]
        table_name = table.model.__name__
        sql_table = Table(table_name)
        insert_query = (
            Query.into(sql_table)
            .columns(*columns)
            .insert(*(Parameter("?") for _ in columns))
        )
        sql = insert_query.get_sql(quote_char='"') + ';'

        pk_columns = list(table.primary_key)
        if not pk_columns:
            raise ValueError(f"Table {table_name} does not define primary key")
        auto_increment = set(table.auto_increment_columns)

        results: list[ModelT] = []
        step = batch_size if batch_size and batch_size > 0 else len(payloads)
        start = 0
        connection = self._acquire_connection()
        while start < len(payloads):
            end = min(start + step, len(payloads))
            subset_payloads = payloads[start:end]
            subset_params = params_matrix[start:end]
            if not subset_params:
                start = end
                continue
            cursor = connection.executemany(sql, [tuple(param) for param in subset_params])
            connection.commit()
            generated_start: int | None = None
            if len(pk_columns) == 1 and pk_columns[0] in auto_increment:
                last_rowid_result = connection.execute("SELECT last_insert_rowid()").fetchone()
                if last_rowid_result is None or last_rowid_result[0] is None:
                    raise RuntimeError("Unable to determine lastrowid for bulk insert")
                last_id = int(last_rowid_result[0])
                generated_start = last_id - len(subset_payloads) + 1

            for offset, payload in enumerate(subset_payloads):
                pk_filter: dict[str, object] = {}
                for pk in pk_columns:
                    value = payload.get(pk)
                    if value is None and generated_start is not None and pk == pk_columns[0]:
                        value = generated_start + offset
                        payload[pk] = value
                    if value is None:
                        raise ValueError(f"Primary key column '{pk}' is null after insert")
                    pk_filter[pk] = value
                instance = self._fetch_single(table, pk_filter, include=None)
                self._invalidate_backrefs(table, instance)
                results.append(instance)
            start = end
        return results

    def find_many(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[ModelT]:
        table_name = table.model.__name__
        sql_table = Table(table_name)
        select_query = Query.from_(sql_table).select(*[sql_table.field(col) for col in table.columns])
        params: list[Any] = []

        if where:
            for column, value in where.items():
                if column not in table.columns:
                    raise KeyError(f"Unknown column '{column}' in where clause")
                field = sql_table.field(column)
                if value is None:
                    select_query = select_query.where(field.isnull())
                else:
                    select_query = select_query.where(field == Parameter("?"))
                    params.append(value)

        if order_by:
            for column, direction in order_by:
                if column not in table.columns:
                    raise KeyError(f"Unknown column '{column}' in order_by clause")
                direction_lower = direction.lower()
                if direction_lower not in {"asc", "desc"}:
                    raise ValueError("order_by direction must be 'asc' or 'desc'")
                select_query = select_query.orderby(sql_table.field(column), order=Order[direction_lower])

        if skip is not None:
            select_query = select_query.offset(skip)
        if take is not None:
            select_query = select_query.limit(take)

        sql = select_query.get_sql(quote_char='"') + ";"
        rows = self._fetch_all(sql, params)
        include_map = include or {}
        return [self._row_to_model(table, row, include_map) for row in rows]

    def find_first(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        *,
        where: WhereT | None = None,
        include: Mapping[str, bool] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        skip: int | None = None,
    ) -> ModelT | None:
        results = self.find_many(
            table,
            where=where,
            include=include,
            order_by=order_by,
            take=1,
            skip=skip,
        )
        return results[0] if results else None

    def _fetch_single(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        where: Mapping[str, object],
        include: Mapping[str, bool] | None,
    ) -> ModelT:
        results = self.find_many(table, where=cast(WhereT, where), include=include)
        if not results:
            raise RuntimeError("Inserted row could not be reloaded")
        return results[0]

    def _normalize_insert_payload(
        self,
        table: TableProtocol[ModelT, InsertT, WhereT],
        data: InsertT | Mapping[str, object],
    ) -> dict[str, object]:
        allowed = set(table.columns)
        if isinstance(data, Mapping):
            return {key: data[key] for key in allowed if key in data}
        insert_model = getattr(table, "insert_model", None)
        if insert_model and isinstance(data, insert_model):
            return {column: getattr(data, column) for column in table.columns if hasattr(data, column)}
        if is_dataclass(data):
            return {column: getattr(data, column) for column in table.columns if hasattr(data, column)}
        raise TypeError("Unsupported insert payload type")

    def _fetch_all(self, sql: str, params: Sequence[Any]) -> list[sqlite3.Row]:
        cursor = self._execute(sql, params)
        return cursor.fetchall()

    def _execute(self, sql: str, params: Sequence[Any], auto_commit: bool = True) -> sqlite3.Cursor:
        connection = self._acquire_connection()
        cursor = connection.execute(sql, tuple(params))
        if auto_commit:
            connection.commit()
        return cursor

    def _acquire_connection(self) -> sqlite3.Connection:
        if self._factory is None:
            assert self._connection is not None
            self._ensure_row_factory(self._connection)
            return self._connection

        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = self._factory()
            if not isinstance(connection, sqlite3.Connection):
                raise TypeError("SQLite backend factory must return sqlite3.Connection")
            self._ensure_row_factory(connection)
            self._local.connection = connection
        return connection

    @staticmethod
    def _ensure_row_factory(connection: sqlite3.Connection) -> None:
        if connection.row_factory is None:
            connection.row_factory = sqlite3.Row

    def _row_to_model(
        self,
        table: TableProtocol[ModelT, Any, Any],
        row: sqlite3.Row,
        include_map: Mapping[str, bool],
    ) -> ModelT:
        key = self._identity_key(table, row)
        if key is not None:
            cached = self._identity_map.get(key)
        else:
            cached = None
        model = table.model
        if cached is None:
            if is_dataclass(model):
                values: dict[str, Any] = {column: row[column] for column in table.columns}
                instance = model.__new__(model)
                for field in fields(model):
                    if field.name in values:
                        value = values[field.name]
                    elif field.default is not MISSING:
                        value = field.default
                    elif field.default_factory is not MISSING:  # type: ignore[attr-defined]
                        value = field.default_factory()  # type: ignore[misc]
                    else:
                        origin = get_origin(field.type)
                        if origin in (list, set, frozenset):
                            value = origin()  # type: ignore[call-arg]
                        else:
                            value = None
                    object.__setattr__(instance, field.name, value)
            else:
                instance = cast(ModelT, model(**{column: row[column] for column in table.columns}))
            if key is not None:
                self._identity_map[key] = instance
        else:
            instance = cast(ModelT, cached)
            for column in table.columns:
                object.__setattr__(instance, column, row[column])
        self._attach_relations(table, instance, include_map)
        return instance

    def _invalidate_backrefs(
        self,
        table: TableProtocol[ModelT, Any, Any],
        instance: ModelT,
    ) -> None:
        foreign_keys = getattr(table, "foreign_keys", ())
        if not foreign_keys:
            return
        for fk in foreign_keys:
            backref = getattr(fk, "backref", None)
            if not backref:
                continue
            remote_model = fk.remote_model
            if remote_model is None:
                continue
            key_values: list[Any] = []
            for local_col, remote_col in zip(fk.local_columns, fk.remote_columns):
                value = getattr(instance, local_col, None)
                if value is None:
                    key_values = []
                    break
                key_values.append(value)
            if not key_values:
                continue
            identity_key = (remote_model, tuple(key_values))
            owner = self._identity_map.get(identity_key)
            if owner is None:
                continue
            state_map = _LAZY_RELATION_STATE.get(owner)
            if state_map is None:
                continue
            state = state_map.get(backref)
            if state is None:
                continue
            state.loaded = False
            state.loading = False
            state.value = [] if state.many else None

    def _identity_key(
        self,
        table: TableProtocol[ModelT, Any, Any],
        row: sqlite3.Row,
    ) -> tuple[type[Any], tuple[Any, ...]] | None:
        pk_columns = getattr(table, "primary_key", ())
        if not pk_columns:
            return None
        values: list[Any] = []
        for column in pk_columns:
            value = row[column]
            if value is None:
                return None
            values.append(value)
        return (table.model, tuple(values))

    def _attach_relations(
        self,
        table: TableProtocol[ModelT, Any, Any],
        instance: ModelT,
        include_map: Mapping[str, bool],
    ) -> None:
        relations_attr = cast(Sequence[Any], getattr(table, "relations", ()))
        relations: list[RelationSpec] = []
        for entry in relations_attr:
            if isinstance(entry, RelationSpec):
                relations.append(entry)
            elif isinstance(entry, dict):
                relations.append(
                    RelationSpec(
                        name=entry['name'],
                        table_name=entry['table_name'],
                        table_module=entry.get('table_module', table.__class__.__module__),
                        many=bool(entry['many']),
                        mapping=tuple(tuple(pair) for pair in entry['mapping']),
                    )
                )
            else:
                raise TypeError("Unsupported relation specification")

        existing_names: set[str] = {spec.name for spec in relations}
        relations.extend(_find_backref_relations(table, existing_names))
        if not relations:
            return
        backend_for_lazy = cast(BackendProtocol[Any, Any, Mapping[str, object]], self)

        base_model_cls = cast(type[Any], getattr(instance.__class__, "__lazy_base__", instance.__class__))
        lazy_cls = _ensure_lazy_model_class(base_model_cls)
        if instance.__class__ is not lazy_cls:
            instance.__class__ = lazy_cls

        states = _LAZY_RELATION_STATE.get(instance)
        if states is None:
            states = {}

        for spec in relations:
            name = spec.name
            table_module_name = spec.table_module or table.__class__.__module__
            module = sys.modules.get(table_module_name)
            if module is None:
                raise RuntimeError(f"Module '{table_module_name}' not loaded for relation '{name}'")
            table_cls_name = spec.table_name
            table_cls = getattr(module, table_cls_name)
            mapping = spec.mapping
            many = spec.many
            existing_state = states.get(name)
            if existing_state is None:
                state = LazyRelationState(
                    attribute=name,
                    backend=backend_for_lazy,
                    table_cls=cast(type[Any], table_cls),
                    mapping=mapping,
                    many=many,
                    model_cls=cast(type[Any] | None, getattr(table_cls, "model", None)),
                )
                states[name] = state
            else:
                existing_state.backend = backend_for_lazy
                existing_state.table_cls = cast(type[Any], table_cls)
                existing_state.mapping = mapping
                existing_state.many = many
                existing_state.model_cls = cast(type[Any] | None, getattr(table_cls, "model", None))
                state = existing_state

        if not states:
            return
        _LAZY_RELATION_STATE[instance] = states

        for name, state in states.items():
            if include_map.get(name):
                _resolve_lazy_relation(instance, state)

    def close(self) -> None:
        if self._factory is None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._identity_map.clear()
            return
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            delattr(self._local, "connection")
        self._identity_map.clear()


def create_backend(provider: str, connection: Any) -> BackendProtocol[Any, Any, Mapping[str, object]]:
    if isinstance(connection, SQLiteBackend):
        return connection
    if provider == "sqlite":
        return SQLiteBackend(connection)
    raise ValueError(f"Unsupported provider '{provider}'")
