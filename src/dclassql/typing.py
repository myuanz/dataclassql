from typing import Mapping, Literal, TypeVar

ModelT   = TypeVar("ModelT")
InsertT  = TypeVar("InsertT", default=Mapping[str, object])
WhereT   = TypeVar("WhereT",  bound=Mapping[str, object], default=Mapping[str, object])
IncludeT = TypeVar("IncludeT", bound=Mapping[str, bool], default=Mapping[str, bool])
OrderByT = TypeVar("OrderByT", bound=Mapping[str, Literal['asc','desc']], default=Mapping[str, Literal['asc','desc']])
UpsertWhereT = TypeVar("UpsertWhereT", bound=Mapping[str, object], default=Mapping[str, object])
