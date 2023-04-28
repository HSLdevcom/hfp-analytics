"""
Services related to /stops data endpoint
"""
from typing import List, Tuple, Optional
from common.database import pool


async def get_stops(stop_id: Optional[int] = None) -> List[Tuple]:
    """Return jore stops filtered optionally by stop id"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT * FROM api.view_jore_stop_4326
                WHERE (
                    %(stop_id)s::int IS NULL OR
                    CAST (st_asgeojson -> 'properties' ->> 'stop_id' as INTEGER) = %(stop_id)s
                )
                """,
                {"stop_id": stop_id},
            )
            print()
            res = await cur.fetchall()
    return res


async def is_stops_table_empty() -> bool:
    """Check if the jore stop table is empty"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT NOT EXISTS(SELECT * FROM api.view_jore_stop_4326 LIMIT 1)")
            res = await cur.fetchone()
    return not res[0] if res else False


async def get_medians(stop_id: int) -> List[Tuple]:
    """Return analyzed stop medians"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT api.get_percentile_circles_with_stop_id(%(stop_id)s)", {"stop_id": stop_id})
            res = await cur.fetchall()
    return res


async def get_observations():
    pass


async def get_percentiles(stop_id: int) -> List[Tuple]:
    """Return stop analysis percentiles"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT api.get_percentile_circles_with_stop_id(%(stop_id)s)", {"stop_id": stop_id})
            res = await cur.fetchall()
    return res
