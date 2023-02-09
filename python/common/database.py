from psycopg_pool import AsyncConnectionPool
from common.utils import get_conn_params

pool = AsyncConnectionPool(get_conn_params(), max_size=20)
