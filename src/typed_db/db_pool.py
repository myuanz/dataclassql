import functools
import sqlite3
import threading
from typing import Callable, Concatenate, Protocol


class HasLocalClass(Protocol):
    _local: threading.local

def save_local[C: HasLocalClass, **P, T](func: Callable[Concatenate[type[C], P], T]) -> Callable[Concatenate[type[C], P], T]:
    @functools.wraps(func)
    def wrapper(cls: type[C], *args: P.args, **kwargs: P.kwargs) -> T:
        field_name = func.__name__
        if hasattr(cls._local, field_name):
            return getattr(cls._local, field_name)

        r = func(cls, *args, **kwargs)
        setattr(cls._local, field_name, r)
        return r
    return wrapper


class BaseDBPool:
    ''' Thread-level database pool base class. Methods decorated with `@save_local` are cached in `threading.local()`. Usage example:
    ```python
    class ExampleDBPool(BaseDBPool):
        sqlite_db_path = 'data/news.db'
        visitor_sqlite_db_path = 'data/visitors.db'

        @classmethod
        @save_local
        def sqlite_conn(cls) -> sqlite3.Connection:
            conn = sqlite3.connect(cls.sqlite_db_path, check_same_thread=False)
            cls._setup_sqlite_db(conn)
            return conn

        @classmethod
        @save_local
        def fastlite_conn(cls):
            from fastlite import database
            fastlite_db = database(cls.sqlite_db_path)
            return fastlite_db

        @classmethod
        @save_local
        def fastlite_conn_visitor(cls):
            from fastlite import database
            fastlite_db_visitor = database(cls.visitor_sqlite_db_path)
            cls._setup_sqlite_db(fastlite_db_visitor.conn)
            return fastlite_db_visitor
    ```
    '''

    _local = threading.local()

    @classmethod
    def close_all(cls, verbose: bool = False):
        for attr in dir(cls._local):
            if '_conn' in attr or '_db' in attr:
                if verbose:
                    print(f'Check {attr}')
                obj = getattr(cls._local, attr)
                if hasattr(obj, 'close') and callable(obj.close):
                    if verbose:
                        print(f'\tClosing {attr}')
                    obj.close()
                delattr(cls._local, attr)

    @classmethod
    def _setup_sqlite_db(cls, conn: sqlite3.Connection):
        conn.execute('PRAGMA journal_mode = WAL;')
        conn.execute('PRAGMA synchronous = NORMAL;')
        conn.execute('pragma temp_store = memory;')
        conn.execute('pragma page_size = 32768;')
        conn.execute("PRAGMA busy_timeout = 3000;")
        conn.execute('PRAGMA journal_size_limit=104857600;')
