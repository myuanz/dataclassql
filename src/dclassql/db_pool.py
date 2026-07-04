import functools
import sqlite3
import threading
from typing import Any, Callable, Concatenate, Protocol


class HasLocalClass(Protocol):
    _local: threading.local


def save_local[C: HasLocalClass, **P, T](
    func: Callable[Concatenate[C, P], T] | None = None,
    *,
    key: Callable[[Any, Callable[..., object]], object] | None = None,
) -> Callable[[Callable[Concatenate[C, P], T]], Callable[Concatenate[C, P], T]] | Callable[Concatenate[C, P], T]:
    def decorator(func: Callable[Concatenate[C, P], T]) -> Callable[Concatenate[C, P], T]:
        @functools.wraps(func)
        def wrapper(self: C, *args: P.args, **kwargs: P.kwargs) -> T:
            cache = getattr(self._local, "_dclassql_cache", None)
            if cache is None:
                cache = {}
                self._local._dclassql_cache = cache

            cache_key = key(self, func) if key is not None else func.__name__
            if cache_key in cache:
                return cache[cache_key]

            value = func(self, *args, **kwargs)
            cache[cache_key] = value
            return value

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


class BaseDBPool:
    ''' Thread-level database pool base class. Methods decorated with `@save_local` are cached in `threading.local()`. Usage example:
    ```python
    class ExampleDBPool(BaseDBPool):
        sqlite_db_path = 'data/news.db'
        visitor_sqlite_db_path = 'data/visitors.db'

        @save_local
        def sqlite_conn(self) -> sqlite3.Connection:
            conn = sqlite3.connect(self.sqlite_db_path, check_same_thread=False)
            self._setup_sqlite_db(conn)
            return conn

        @save_local
        def fastlite_conn(self):
            from fastlite import database
            fastlite_db = database(self.sqlite_db_path)
            return fastlite_db

        @save_local
        def fastlite_conn_visitor(self):
            from fastlite import database
            fastlite_db_visitor = database(self.visitor_sqlite_db_path)
            self._setup_sqlite_db(fastlite_db_visitor.conn)
            return fastlite_db_visitor
    ```
    '''

    _local = threading.local()

    @classmethod
    def close_all(cls, verbose: bool = False):
        cache = getattr(cls._local, "_dclassql_cache", None)
        if cache is None:
            return
        for key, obj in list(cache.items()):
            label = repr(key)
            if hasattr(obj, 'close') and callable(obj.close):
                if verbose:
                    print(f'Closing {label}')
                obj.close()
            del cache[key]

    @classmethod
    def _setup_sqlite_db(cls, conn: sqlite3.Connection):
        conn.execute('PRAGMA journal_mode = WAL;')
        conn.execute('PRAGMA synchronous = NORMAL;')
        conn.execute('pragma temp_store = memory;')
        conn.execute('pragma page_size = 32768;')
        conn.execute("PRAGMA busy_timeout = 3000;")
        conn.execute('PRAGMA journal_size_limit=104857600;')
