from psycopg_pool import AsyncConnectionPool
from common.config import POSTGRES_CONNECTION_STRING

pool = AsyncConnectionPool(POSTGRES_CONNECTION_STRING, max_size=20)
