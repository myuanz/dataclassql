from typing import Mapping, Literal, TypeVar

type OrderDirection = Literal["asc", "desc"]

ModelT   = TypeVar("ModelT")
InsertT  = TypeVar("InsertT", default=Mapping[str, object])
WhereT   = TypeVar("WhereT",  bound=Mapping[str, object], default=Mapping[str, object])
IncludeT = TypeVar("IncludeT", bound=Mapping[str, bool], default=Mapping[str, bool])
OrderByT = TypeVar("OrderByT", bound=Mapping[str, OrderDirection], default=Mapping[str, OrderDirection])
UpsertWhereT = TypeVar("UpsertWhereT", bound=Mapping[str, object], default=Mapping[str, object])
