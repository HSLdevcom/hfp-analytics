import logging
from datetime import date

import common.constants as constants
import httpx
import psycopg2
from common.config import DIGITRANSIT_APIKEY, POSTGRES_CONNECTION_STRING
from common.preprocess import check_preprocessed_files, load_delay_hfp_data, preprocess
from common.utils import get_target_oday

start_time = 0
GRAPHQL_URL = 'https://api.digitransit.fi/routing/v2/hsl/gtfs/v1'
logger = logging.getLogger('importer')


def create_route_query(mode: str) -> str:
    return f"""
    {{
        routes(transportModes: [{mode}]) {{
            gtfsId
        }}
    }}
    """

async def get_query_async(query):
    async with httpx.AsyncClient() as client:
        req = await client.post(
            url=GRAPHQL_URL,
            content=query,
            headers={
                "Content-Type": "application/graphql",
                "digitransit-subscription-key": DIGITRANSIT_APIKEY,
            }
        )
    if req.status_code == 200:
        return req.json()
    else:
        raise Exception(f'{req} failed with status code {req.status_code}')

async def run_delay_analysis(requested_oday: date = None):
    conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
    try:
        with conn:
            with conn.cursor() as cur:

                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked:
                    logger.warn("Importer is LOCKED which means that importer should be already running. You can get"
                                "rid of the lock by restarting the database if needed.")
                    return

                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))

                bus_query = create_route_query("BUS")
                tram_query = create_route_query("TRAM")
                bus_routes_res = await get_query_async(bus_query)
                tram_routes_res = await get_query_async(tram_query)
                
                bus_route_ids = [route["gtfsId"].split(":")[1] for route in bus_routes_res["data"]["routes"]]
                tram_route_ids = [route["gtfsId"].split(":")[1] for route in tram_routes_res["data"]["routes"]]

                route_ids = bus_route_ids + tram_route_ids

                filtered_route_ids = [r for r in route_ids if not (r.endswith("N") or r.endswith("H"))]
                filtered_route_ids.sort()
                oday = requested_oday
                if requested_oday is None:
                    oday = get_target_oday()

                for i, route_id in enumerate(filtered_route_ids, start=1):
                    preprocessed_routes_exist = await check_preprocessed_files(route_id, oday, "preprocess_clusters")
                    preprocessed_modes_exist = await check_preprocessed_files(route_id, oday, "preprocess_departures")

                    if preprocessed_routes_exist and preprocessed_modes_exist:
                        logger.debug(f"[{i}/{len(filtered_route_ids)}] Preprocessed files for {oday} for route_id={route_id} exists. Skipping.")
                        continue

                    df = await load_delay_hfp_data(route_id, oday)
                    logger.debug(f"[{i}/{len(filtered_route_ids)}] Data fetched from oday {oday} for route_id={route_id}. Running preprocess.")

                    try:
                        await preprocess(df, route_id, oday)
                    except ValueError as e:
                        logger.debug(f"[{i}/{len(filtered_route_ids)}] Preprocessing failed for route_id={route_id}, skipping. Error: {e}")
                        continue
                    
                    logger.debug(f"[{i}/{len(filtered_route_ids)}] Preprocessed {route_id}.")
                
                logger.debug(f"Http triggered preprocessing done for {len(filtered_route_ids)} routes for oday {oday}.")


    except Exception:
        logger.exception("Analysis failed.")
    finally:
        conn.cursor().execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
        conn.close()