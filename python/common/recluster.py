
# TODO: clean up imports
import io
import asyncio
import functools
import pandas as pd
import numpy as np
import shutil
import glob
import os
import yaml
import geopandas as gpd
import psycopg
import warnings
import zstandard as zstd
import logging
import time
import gc

from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from common.database import pool
from common.logger_util import CustomDbLogHandler
from common.utils import get_season
from common.config import DAYS_TO_EXCLUDE
from common.container_client import FlowAnalyticsContainerClient
from sklearn.cluster import DBSCAN
from typing import Dict, Any, Union, Optional, List, Literal

logger = logging.getLogger("api")

# TODO: Move to configs
EPS_DISTANCE_1 = 0.01
EPS_DISTANCE_2 = 0.5
EARHT_RADIUS_KM = 6371
DEPARTURE_THRESHOLD = 5
MIN_MEDIAN_DELAY_IN_CLUSTER = 15
HDG_DIFF_LOWER_LIMIT = 170
HDG_DIFF_UPPER_LIMIT = 190
MIN_DELAY_EVENTS = 5
MIN_WEIGHTED_SAMPLES = 60
SPEEDS_IN_DELAY = ['DELAY', 'SLOW']
SPEED_CLASSES = {
    "DELAY": {
        "MIN": 0.27,
    },
    "SLOW": {
        "LOWER": 0.27,
        "UPPER": 0.83,
    },
    "FAST": {
        "MAX": 0.83,
    },
}

DCLASS_NAMES = {
    "on_route": "Ajo",
    "pass": "Ohitus",
    "arr": "Saapuminen",
    "dep": "Poistuminen",
    "stop": "Pysakki"
}

SEASON_MONTHS = {
  "WINTER": [12, 1, 2],
  "SPRING": [3, 4, 5],
  "SUMMER": [6, 7, 8],
  "AUTUMN": [9, 10, 11]
}

def get_routes_condition(column: str, values: list[str]) -> tuple[str, dict]:
    placeholders = []
    params = {}
    for i, val in enumerate(values):
        key = f"{column}{i}"
        placeholders.append(f"%({key})s")
        params[key] = val
    condition = f"{column} IN ({', '.join(placeholders)})"
    return condition, params

# Refactor with load_compressed_departures_csv
async def load_preprocess_files(
    route_ids: Optional[List[str]],
    from_oday: str,
    to_oday: str,
    table: str
) -> bytes:
    base_query = f"SELECT zst FROM delay.{table}"
    conditions = []
    params = {}
    conditions.append("oday >= %(from_oday)s::date")
    conditions.append("oday <= %(to_oday)s::date")
    params["from_oday"] = from_oday
    params["to_oday"] = to_oday

    if route_ids and route_ids != "ALL":
        in_condition, in_params = get_routes_condition("route_id", route_ids)
        conditions.append(in_condition)
        params.update(in_params)

    query = base_query
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    async with pool.connection() as conn:
        row = await conn.execute(query, params)
        results = await row.fetchall()

    if not results:
        return None 

    dfs = []
    decompressor = zstd.ZstdDecompressor()
    for r in results:
        compressed_data = r[0]  
        decompressed_csv = decompressor.decompress(compressed_data)
        df = pd.read_csv(io.BytesIO(decompressed_csv), sep=";")
        dfs.append(df)

    if not dfs:
        return None 

    combined_df = pd.concat(dfs, ignore_index=True)

    # TODO: If recluster analysis has excluded days how to handle it in db
    # Currently if analysis exists but excluding removes all rows the existing analysis is returned
    excluded_odays = DAYS_TO_EXCLUDE
    if excluded_odays:
        combined_df = combined_df[~combined_df["oday"].isin(excluded_odays)]

    if combined_df.empty:
        return None

    buffer = io.BytesIO()
    combined_df.to_csv(buffer, sep=";", index=False)
    buffer.seek(0)
    return buffer.getvalue()


async def get_recluster_status(table: str, from_oday: str, to_oday: str, route_id: str = "ALL",) -> Dict[str, Optional[Any]]:
    table_name = f"delay.{table}"
    query = f"""
        SELECT status, createdAt, modifiedAt
        FROM {table_name}
        WHERE route_id = %(route_id)s AND from_oday = %(from_oday)s AND to_oday = %(to_oday)s
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            query,
            {
                "route_id":   route_id,
                "from_oday":  from_oday,
                "to_oday":    to_oday,
            }
        )
        row = await cur.fetchone()

    if row is None:
        return {"status": None, "createdAt": None, "modifiedAt": None}

    status, created_at, modified_at = row 
    return {"status": status, "createdAt": created_at, "modifiedAt": modified_at}

async def set_recluster_status(
    table: str,
    from_oday: date,
    to_oday:   date,
    route_id: str,
    status:    Literal["PENDING", "DONE", "FAILED"] = "PENDING",
) -> None:
    table_name = f"delay.{table}"
    query = f"""
        INSERT INTO {table_name} (route_id, from_oday, to_oday, status)
        VALUES (%(route_id)s, %(from_oday)s, %(to_oday)s, %(status)s)
        ON CONFLICT (route_id, from_oday, to_oday)
        DO UPDATE
          SET status    = EXCLUDED.status,
              createdAt = now();
    """
    async with pool.connection() as conn:
        await conn.execute(
            query,
            {
                "route_id":  route_id,
                "from_oday": from_oday,
                "to_oday":   to_oday,
                "status":    status,
            }
        )

async def load_recluster_files(table: str, from_oday: str, to_oday: str, route_id: str = "ALL",) -> bytes:
    table_name = f"delay.{table}"
    query = f"""
        SELECT zst
        FROM {table_name}
        WHERE route_id = %(route_id)s AND from_oday = %(from_oday)s AND to_oday = %(to_oday)s
    """
    async with pool.connection() as conn:
        row = await conn.execute(
            query,
            {
                "route_id": route_id,
                "from_oday": from_oday,
                "to_oday": to_oday
            }
        )
        result = await row.fetchone()
        if not result or not result[0]:
            return None 

        compressed_data = result[0]

    dctx = zstd.ZstdDecompressor()
    decompressed_geojson = dctx.decompress(compressed_data)
    return decompressed_geojson

def run_asyncio_task(coro_fn, *args, **kwargs):
    return asyncio.run(coro_fn(*args, **kwargs))


async def run_analysis_and_set_status(
    table: str,
    route_ids: list[str],
    from_oday: date,
    to_oday: date,
):
    with CustomDbLogHandler("api"):
        try:
            logger.debug(f"Start asyncio task to run recluster analysis")
            await asyncio.to_thread(
                functools.partial(run_asyncio_task, recluster_analysis, route_ids, from_oday, to_oday)
            )
        except Exception:
            logger.debug(f"Something went wrong. Setting status as FAILED")
            await set_recluster_status(table, from_oday, to_oday, route_ids, status="FAILED")
            raise
        finally:
            gc.collect()

async def store_compressed_geojson(
    table: str,
    route_id: str,
    from_oday: str,
    to_oday: str,
    gdf: gpd.GeoDataFrame,
    flow_analytics_container_client: FlowAnalyticsContainerClient,
):
    """
    Convert the GeoDataFrame to GeoJSON and compress with zstd.
    Saves compressed data to database and to blob storage
    """

    for col in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
            gdf[col] = gdf[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    geojson_str = gdf.to_json() 
    
    geojson_bytes = geojson_str.encode("utf-8")
    cctx = zstd.ZstdCompressor()
    compressed_data = cctx.compress(geojson_bytes)

    table_name = f"delay.{table}"

    query = f"""
        INSERT INTO {table_name} (route_id, from_oday, to_oday, zst)
        VALUES (%(route_id)s, %(from_oday)s, %(to_oday)s, %(zst)s)
        ON CONFLICT (route_id, from_oday, to_oday) DO UPDATE
            SET zst = EXCLUDED.zst,
                status = 'DONE',
                modifiedAt = now();
    """

    async with pool.connection() as conn:
        await conn.execute(
            query,
            {
                "route_id": route_id,
                "from_oday": from_oday,
                "to_oday": to_oday,
                "zst": compressed_data
            }
        )
    
    recluster_type = table.split("_")[1]

    await flow_analytics_container_client.save_cluster_data(
        recluster_type=recluster_type,
        compressed_data=compressed_data,
        from_oday=from_oday.strftime("%Y-%m-%d"),
        to_oday=to_oday.strftime("%Y-%m-%d"),
        route_id=','.join(route_id) if type(route_id) == list else route_id,
    )   


def make_geo_df_WGS84(df: pd.DataFrame, lat_col: str, lon_col: str, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Make a geodf from df. Note thet the function does not convert CRS but your input df needs to be WGS84,
    ie EPSG:4326.

    Args:
        df (pd.DataFrame): _description_
        lat_col (str): _description_
        lon_col (str): _description_
        crs (_type_, optional): _description_. Defaults to "EPSG:4326".

    Returns:
        gpd.GeoDataFrame: _description_
    """
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs=crs)
    return gdf

def recluster(
    clusters: pd.DataFrame,
    distance: int,
    radius: int,
    min_weighted_samples: int,
    vars_to_group_level_one_clusters_by=['route_id', 'direction_cluster_id', 'time_group', 'dclass'],
    cluster_id_vars_on_2nd_level=['route_id', 'direction_id', 'time_group', 'dclass', 'cluster_on_reclustered_level']
) -> pd.DataFrame:

    g = clusters.groupby(vars_to_group_level_one_clusters_by)

    departure_clusters = []
    reclustered_clusters = []
    EPSILON = distance / radius
    logger.debug(f"Data to be procecesed with DBSCAN. Rows: {clusters.shape[0]}, groups: {g.ngroups}")
    for i, (group_key, sub) in enumerate(g, start=1):
        sub = sub.rename(columns={"cluster": "cluster_on_departure_level"})
        X = np.radians(sub[["lat_median", "long_median"]])

        clusterer = DBSCAN(
            eps=EPSILON,
            min_samples=min_weighted_samples,  # The number of samples (or total weight) in a neighborhood for a point to be considered as a core point.
            metric="haversine",
        )

        sub["cluster_on_reclustered_level"] = clusterer.fit_predict(X, sample_weight=sub["weight"])
        sub = sub[sub["cluster_on_reclustered_level"] != -1]
        if sub.empty:
            continue

        departure_clusters.append(sub)
        sub = calculate_cluster_features(sub, cluster_id_vars_on_2nd_level)
        reclustered_clusters.append(sub)

        if i % 1000 == 0:
            del sub
            gc.collect()
            logger.debug(f"DBSCAN processed {i}/{g.ngroups} groups")

    departure_clusters = pd.concat(departure_clusters)
    reclustered_clusters = pd.concat(reclustered_clusters)
    gc.collect()

    return reclustered_clusters, departure_clusters


def calculate_cluster_features(df: pd.DataFrame, cluster_id_vars_on_2nd_level: list) -> pd.DataFrame:
    """Calculate additional features for the identified clusters: medians for location and time and descriptive
    values for the deviation of the delay.
    Note:
    - nrows = ndeps
    - weight is the weighted value of delay seconds

    Args:
        df pd.DataFrame: _description_
        cluster_id_vars_on_2nd_level (list, optional): _description_. Defaults to ['route_id','direction_id','time_group','dclass','cluster_on_reclustered_level'].

    Returns:
        pd.DataFrame: clusters with descriptive variables
    """

    df["tst_median"] = pd.to_datetime(df["tst_median"], format="ISO8601")
    df["oday"] = pd.to_datetime(df["oday"])

    clust_counts = df.drop_duplicates(
        subset=[
            "route_id",
            "direction_id",
            "oday",
            "start",
            "cluster_on_reclustered_level",
        ]
    )
    clust_counts = clust_counts.groupby(cluster_id_vars_on_2nd_level).size().reset_index(name="n_departures")

    clust_delay_feats = df.groupby(cluster_id_vars_on_2nd_level)["weight"].quantile([0.10, 0.25, 0.5, 0.75, 0.90]).unstack()
    clust_delay_feats.columns = [(int(x * 100)) for x in clust_delay_feats.columns]
    clust_delay_feats = clust_delay_feats.add_prefix("q_").reset_index()
    median_vars = df.groupby(cluster_id_vars_on_2nd_level)[["lat_median", "long_median", "tst_median", "hdg_median"]].median().reset_index()
    res = median_vars.merge(clust_counts, on=cluster_id_vars_on_2nd_level, how="outer")
    res = res.merge(clust_delay_feats, on=cluster_id_vars_on_2nd_level, how="outer")
    res["oday_min"] = df.oday.min()
    res["oday_max"] = df.oday.max()
    return res


def ui_related_var_modifications(df: pd.DataFrame, seasons_and_months: dict, DEPARTURE_THRESHOLD: int) -> pd.DataFrame:
    """All UI specific stuff here.
    Args:
        df: output data to modify
        seasons_and_months dict: form configs dictionary which maps months to seasons
        DEPARTURE_THRESHOLD:
        configs
    Returns:
        pd.DataFrame: clusters with ui related variables
    """
    df["tst_median"] = pd.to_datetime(df["tst_median"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df["year"] = df["tst_median"].dt.year
    df["season"] = df["tst_median"].dt.month.map(lambda x: get_season(x, seasons_and_months))

    for k, v in DCLASS_NAMES.items():
        df["dclass"] = df["dclass"].replace(k, v)

    # mediaanin luokat avoimella ylärajalla # TODO: testaa yhdellä np.wherellä: lista ehtoja
    df["q_50_category"] = np.where(df["q_50"] <= 30, "0_15_30", ">75")
    df["q_50_category"] = np.where((df["q_50"] > 30) & (df["q_50"] <= 45), "1_30_45", df["q_50_category"])
    df["q_50_category"] = np.where((df["q_50"] > 45) & (df["q_50"] <= 60), "2_45_60", df["q_50_category"])
    df["q_50_category"] = np.where((df["q_50"] > 60) & (df["q_50"] <= 75), "3_60_74", df["q_50_category"])

    # lähtömäärien luokat avoimella ylärajalla
    df["n_departures_category"] = np.where(df["n_departures"] <= DEPARTURE_THRESHOLD, "<=" + str(DEPARTURE_THRESHOLD), ">" + str(DEPARTURE_THRESHOLD))
    df = df.rename(columns={"lat_median": "latitude", "long_median": "longitude"})

    return df


async def get_preprocessed_departures(route_ids: [str], from_oday: str, to_oday: str):
    departures_data = await load_preprocess_files(route_ids, from_oday, to_oday, "preprocess_departures")
    if not departures_data:
        logger.debug(f"No preprocessed departures ZST found for route_id={route_ids}")
        return None

    preprocessed_departures = pd.read_csv(io.BytesIO(departures_data), sep=';')

    week_days_df = preprocessed_departures[
        preprocessed_departures["time_group"].str.contains("weekday", case=False, na=False)
    ].copy()
    week_days_df["time_group"] = "0_weekday_all"

    preprocessed_departures = pd.concat([preprocessed_departures, week_days_df], axis=0).reset_index(drop=True)

    return preprocessed_departures

async def get_preprocessed_clusters(route_ids: [str], from_oday: str, to_oday: str):
    cluster_data = await load_preprocess_files(route_ids, from_oday, to_oday, "preprocess_clusters")
    if not cluster_data:
        logger.debug(f"No preprocessed cluster ZST found for route_id={route_ids}")
        return None

    clusters = pd.read_csv(io.BytesIO(cluster_data), sep=";")

    week_days_df = clusters[
        clusters["time_group"].str.contains("weekday", case=False, na=False)
    ].copy()
    week_days_df["time_group"] = "0_weekday_all"

    clusters = pd.concat([clusters, week_days_df], axis=0).reset_index(drop=True)
    return clusters


async def recluster_analysis(route_ids: [str], from_oday: str, to_oday: str):
    with CustomDbLogHandler("api"):
        start_time = datetime.now()
        clusters = await get_preprocessed_clusters(route_ids, from_oday, to_oday)
        preprocessed_departures = await get_preprocessed_departures(route_ids, from_oday, to_oday)

        removal_end = datetime.now()
        logger.debug(f"Data fetched for recluster in {removal_end - start_time}")

        if clusters is None or preprocessed_departures is None:
            return

        start_time = datetime.now()
        logger.debug(f"Start recluster for routes")

        route_clusters, departure_clusters = recluster(
            clusters,
            distance=EPS_DISTANCE_2,
            radius=EARHT_RADIUS_KM,
            min_weighted_samples=MIN_WEIGHTED_SAMPLES,
            vars_to_group_level_one_clusters_by=['route_id', 'direction_id', 'time_group', 'dclass'],
            cluster_id_vars_on_2nd_level=['route_id', 'direction_id', 'time_group', 'dclass', 'cluster_on_reclustered_level']
        )

        n_departures_analyzed = preprocessed_departures.groupby(["route_id", "direction_id", "time_group"]).size().to_frame().reset_index().rename(columns={0: "n_departures_analyzed"})
        route_clusters = route_clusters[route_clusters["q_50"] >= MIN_MEDIAN_DELAY_IN_CLUSTER]
        route_clusters = route_clusters.merge(n_departures_analyzed, how="left", on=["route_id", "direction_id", "time_group"])
        route_clusters["share_of_departures"] = route_clusters["n_departures"] / route_clusters["n_departures_analyzed"] * 100

        departure_clusters = route_clusters[["route_id", "direction_id", "time_group", "dclass", "cluster_on_reclustered_level"]].merge(
            departure_clusters, on=["route_id", "direction_id", "time_group", "dclass", "cluster_on_reclustered_level"], how="inner"
        )

        route_clusters = ui_related_var_modifications(route_clusters, SEASON_MONTHS, DEPARTURE_THRESHOLD)

        route_clusters["route_dir"] = route_clusters["route_id"].astype(str) + " S" + route_clusters["direction_id"].astype(str)
        bins = list(range(0, 101, 20))
        labs = []
        for i in range(len(bins) - 1):
            label = str(bins[i]) + "_" + str(bins[i + 1])
            labs.append(label)
        
        route_clusters["shares_category"] = pd.cut(
            route_clusters["share_of_departures"],
            bins=bins,
            labels=labs,
            include_lowest=True,
        )
        route_clusters["share_of_departures"] = round(route_clusters["share_of_departures"], 1)
        route_clusters = route_clusters.drop("cluster_on_reclustered_level", axis=1)

        route_clusters = make_geo_df_WGS84(route_clusters, lat_col="latitude", lon_col="longitude", crs="EPSG:4326")
        
        db_route_id = route_ids
        if not db_route_id:
            db_route_id = 'ALL'

        flow_analytics_container_client = FlowAnalyticsContainerClient()
        
        removal_end = datetime.now()
        logger.debug(f"Recluster analysis for routes done in {removal_end - start_time}")

        await store_compressed_geojson(
            "recluster_routes",
            db_route_id,
            from_oday,
            to_oday,
            route_clusters,
            flow_analytics_container_client=flow_analytics_container_client,
        )

        del route_clusters, departure_clusters, clusters, preprocessed_departures
        gc.collect()

        # Modes cluster disabled for now
        """logger.debug(f"Recluster routes stored to db. Starting recluster for departures.")
        start_time = datetime.now()

        #assert route_clusters['share_of_departures'].max() <= 100
        #assert route_clusters[route_clusters.duplicated()].empty
        clusters = clusters.merge(preprocessed_departures[['route_id', 'direction_id', 'oday', 'start', 'transport_mode']], how="left", on=['route_id', 'direction_id', 'oday', 'start'])
        n_departures_analyzed = clusters.groupby(["transport_mode", "time_group"]).size().to_frame().reset_index().rename(columns={0: 'n_departures_analyzed'})

        mode_clusters, departure_clusters = recluster(
            clusters,
            distance=EPS_DISTANCE_2,
            radius=EARHT_RADIUS_KM,
            min_weighted_samples=MIN_WEIGHTED_SAMPLES,
            vars_to_group_level_one_clusters_by=["transport_mode", 'time_group', 'dclass'],
            cluster_id_vars_on_2nd_level=["transport_mode", 'time_group', 'dclass', 'cluster_on_reclustered_level']
        )

        removal_end = datetime.now()
        logger.debug(f"Recluster analysis for departures done in {removal_end - start_time}")
        start_time = datetime.now()

        mode_clusters = mode_clusters[mode_clusters["q_50"] >= MIN_MEDIAN_DELAY_IN_CLUSTER]
        mode_clusters = mode_clusters.merge(n_departures_analyzed, how='left', on=['transport_mode', 'time_group'])

        # Keep only departures that contribute to mode level clusters
        departure_clusters = mode_clusters[["transport_mode", "time_group", "dclass", "cluster_on_reclustered_level"]].merge(
            departure_clusters, on=["transport_mode", "time_group", "dclass", "cluster_on_reclustered_level"], how="left"
        )
        departure_clusters = departure_clusters.drop_duplicates(subset=['route_id', 'direction_id', 'oday', 'start', 'tst_median', 'time_group', 'cluster_on_reclustered_level']).reset_index(drop=True)

        departure_clusters["cluster_id"] = (
            departure_clusters['dclass'] + departure_clusters['cluster_on_reclustered_level'].astype(str) + departure_clusters['time_group'] + departure_clusters['transport_mode']
        )
        mode_clusters["cluster_id"] = mode_clusters['dclass'] + mode_clusters['cluster_on_reclustered_level'].astype(str) + mode_clusters['time_group'] + mode_clusters['transport_mode']
                
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            departure_clusters["m_norm_hdg_median"] = departure_clusters.groupby(["dclass", "cluster_on_reclustered_level", "time_group", "transport_mode"])["hdg_median"].transform(
                lambda x: (x - x.median()) / (x.quantile(0.75) - x.quantile(0.25))
            )  # May be NA if median == iqr. Ignore RuntimeWarning in these cases
        
        mode_clusters = mode_clusters.merge(departure_clusters[["cluster_id", "m_norm_hdg_median"]], how="left", on="cluster_id")
    
        # var reprocessing
        mode_clusters = ui_related_var_modifications(mode_clusters, SEASON_MONTHS, DEPARTURE_THRESHOLD)

        mode_clusters['transport_mode'] = mode_clusters['transport_mode'].replace('bus', 'Bussi').replace('tram', 'Raitiovaunu')
        # mode_clusters['share_of_departures'] = mode_clusters['departures'] / mode_clusters['num_of_deps_analyzed'] * 100 # NOTE: This var is redundant ATM
        mode_clusters = mode_clusters.drop('cluster_on_reclustered_level', axis=1)
        mode_clusters = make_geo_df_WGS84(mode_clusters, lat_col="latitude", lon_col="longitude", crs="EPSG:4326")
        # Is there a reason to store this in db and not just return it as response?
        await store_compressed_geojson(
            "recluster_modes",
            db_route_id,
            from_oday,
            to_oday,
            mode_clusters,
            flow_analytics_container_client=flow_analytics_container_client,
        )
        removal_end = datetime.now()
        logger.debug(f"Recluster modes stored to db {removal_end - start_time}.")"""