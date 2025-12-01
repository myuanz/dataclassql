from .asdict import asdict
from .db_pool import BaseDBPool, save_local
from .model_inspector import DataSourceConfig
from .push import db_push
from .runtime.backends.lazy import eager
from .runtime.sql_recorder import record_sql
from .unwarp import unwarp, unwarp_or, unwarp_or_raise


class _MissingClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError(
            "dclassql.Client 尚未生成。请先运行 `dql -m <model.py> generate` 生成客户端后再导入。"
        )

try:  # pragma: no cover - exercised in integration tests
    from .client import Client  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback when未生成
    Client = _MissingClient  # type: ignore[assignment]


__all__ = [
    'Client',
    'db_push',
    'eager',
    'asdict',
    'unwarp',
    'unwarp_or',
    'unwarp_or_raise',
    'BaseDBPool',
    'save_local',
    'DataSourceConfig',
    'record_sql',
]
