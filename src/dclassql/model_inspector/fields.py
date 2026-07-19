from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Self


@dataclass(slots=True, frozen=True)
class FieldTo[T](Mapping[str, T]):
    '''从字段名到T的映射
    
    所有需要从 dataclass/table 的 fields 做映射的地方都应该使用本类'''
    _mapping: Mapping[str, T]

    @staticmethod
    def from_mapping[VT](mapping: Mapping[str, VT]) -> 'FieldTo[VT]':
        return FieldTo(mapping)

    def __getitem__(self, field: str) -> T:
        return self._mapping[field]

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping)

    def __len__(self) -> int:
        return len(self._mapping)
