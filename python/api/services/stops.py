"""
Services related to /stops data endpoint
"""

from typing import List, Optional, Tuple

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
            res = await cur.fetchall()
    return res


async def is_stops_table_empty() -> bool:
    """Check if the jore stop table is empty"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT NOT EXISTS(SELECT * FROM api.view_jore_stop_4326 LIMIT 1)"
            )
            res = await cur.fetchone()
    return not res[0] if res else False


async def get_medians(stop_id: Optional[int] = None) -> List[Tuple]:
    """Return analyzed stop medians"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT * FROM api.view_stop_median_4326
                WHERE (
                    %(stop_id)s::int IS NULL OR
                    CAST (st_asgeojson -> 'properties' ->> 'stop_id' as INTEGER) = %(stop_id)s
                )
                """,
                {"stop_id": stop_id},
            )
            res = await cur.fetchall()
    return res


async def is_stop_medians_table_empty() -> bool:
    """Check if the jore stop medians table is empty"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT NOT EXISTS(SELECT * FROM api.view_stop_median_4326 LIMIT 1)"
            )
            res = await cur.fetchone()
    return not res[0] if res else False


async def get_stop_observations(stop_id: int) -> List[Tuple]:
    """Get HFP observations for a stop"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT api.get_observation_4326(%(stop_id)s)
                """,
                {"stop_id": stop_id},
            )
            res = await cur.fetchall()
    return res


async def get_null_observations_for_stop(
    stop_id: int, search_radius: Optional[int] = 100
) -> List[Tuple]:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT api.get_observations_with_null_stop_id_4326(%(stop_id)s, %(search_radius)s)",
                {"stop_id": stop_id, "search_radius": search_radius},
            )
            res = await cur.fetchall()
    return res


async def get_percentiles(stop_id: int) -> List[Tuple]:
    """Return stop analysis percentiles"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT api.get_percentile_circles_with_stop_id(%(stop_id)s)",
                {"stop_id": stop_id},
            )
            res = await cur.fetchall()
    return res
