from typing import Any

from dclassql.db_pool import BaseDBPool, save_local
from dclassql.model_inspector import DataSourceConfig
from dclassql.push import db_push
from dclassql.push.base import ConfirmRebuildCallback
from dclassql.runtime.backends import BackendProtocol
from dclassql.runtime.backends.protocols import SchemaTableProtocol
from dclassql.runtime.datasource import open_sqlite_connection


class ClientBase(BaseDBPool):
    def __init__(self, *, datasource: DataSourceConfig, echo_sql: bool = False) -> None:
        self.datasource = datasource
        self._echo_sql = echo_sql
        self._backend_instance: BackendProtocol | None = None
        self._tables: tuple[SchemaTableProtocol, ...] = ()

    def _backend(self) -> BackendProtocol:
        if self._backend_instance is None:
            self._backend_instance = self._make_backend(self.datasource)
        return self._backend_instance

    def _make_backend(self, datasource: DataSourceConfig) -> BackendProtocol:
        if datasource.provider == "sqlite":
            from dclassql.runtime.backends.sqlite import SQLiteBackend

            return SQLiteBackend(lambda: self._connection(), echo_sql=self._echo_sql)
        raise ValueError(f"Unsupported provider '{datasource.provider}'")

    @save_local(key=lambda self, func: (func.__name__, self.datasource.identity))
    def _connection(self) -> Any:
        return self._open_connection(self.datasource)

    def _open_connection(self, datasource: DataSourceConfig) -> Any:
        if datasource.provider == "sqlite":
            conn = open_sqlite_connection(datasource.url)
            self._setup_sqlite_db(conn)
            return conn
        raise ValueError(f"Unsupported provider '{datasource.provider}'")

    def push_db(
        self,
        *,
        sync_indexes: bool = False,
        force_rebuild: bool = False,
        confirm_rebuild: ConfirmRebuildCallback | None = None,
    ) -> None:
        db_push(
            self._tables,
            self._connection(),
            provider=self.datasource.provider,
            sync_indexes=sync_indexes,
            confirm_rebuild=(lambda *_: True) if force_rebuild else confirm_rebuild,
        )

    def close(self) -> None:
        if self._backend_instance is not None:
            self._backend_instance.close()
            self._backend_instance = None
        self.close_all()
