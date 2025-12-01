from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

_current_recorder: contextvars.ContextVar[tuple[list[tuple[str, tuple[Any, ...]]], bool] | None] = (
    contextvars.ContextVar("_current_recorder", default=None)
)


def record_sql(echo: bool = False):
    """
    记录当前上下文执行的 SQL.
    example:
    ```python
    with record_sql() as sqls:
        client.user.find_first(id=1)
    print(sqls)  # [('SELECT ...', (1,))]
    ```
    """

    @contextmanager
    def _manager() -> Iterator[list[tuple[str, tuple[Any, ...]]]]:
        token = None
        records: list[tuple[str, tuple[Any, ...]]] = []
        try:
            token = _current_recorder.set((records, echo))
            yield records
        finally:
            if token is not None:
                _current_recorder.reset(token)

    return _manager()


def push_sql(sql: str, params: Sequence[Any], *, echo: bool) -> None:
    recorder = _current_recorder.get()
    rec_echo = False
    if recorder is not None:
        rec_list, rec_echo = recorder
        rec_list.append((sql, tuple(params)))
    if rec_echo or echo:
        print(f"[dclassql] SQL: {sql} | params={list(params)}")
