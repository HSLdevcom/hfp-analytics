from psycopg_pool import AsyncConnectionPool
from common.config import POSTGRES_CONNECTION_STRING

class _AsyncPool:
    def __init__(self, conn_string: str, max_size: int):
        self._conn_string = conn_string
        self._max_size = max_size
        self._pool: AsyncConnectionPool | None = None

    def _ensure_pool(self):
        if self._pool is None:
            self._pool = AsyncConnectionPool(self._conn_string, max_size=self._max_size)

    def __getattr__(self, name):
        self._ensure_pool()
        return getattr(self._pool, name)

pool = _AsyncPool(POSTGRES_CONNECTION_STRING, max_size=20)