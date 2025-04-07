
import io
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


from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from common.database import pool
from common.utils import get_season
from common.config import DAYS_TO_EXCLUDE
from sklearn.cluster import DBSCAN
from typing import Union, Optional

logger = logging.getLogger("api")

EPS_DISTANCE_1 = 0.01
EPS_DISTANCE_2 = 0.5
EARHT_RADIUS_KM = 6371
MIN_MEDIAN_DELAY_IN_CLUSTER = 15
HDG_DIFF_LOWER_LIMIT = 170
HDG_DIFF_UPPER_LIMIT = 190
MIN_DELAY_EVENTS = 5
MIN_WEIGHTED_SAMPLES = 60
MIN_MEDIAN_DELAY_IN_CLUSTER = 15
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
    "on_route": "0_on_route",
    "pass": "1_passing_stop",
    "arr": "2_arriving_to_stop",
    "dep": "3_departing_from_stop",
    "stop": "4_at_stop"
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
async def load_compressed_cluster_csv(route_ids: [str], from_oday: str, to_oday: str) -> bytes:
    base_query = "SELECT zst FROM delay.preprocess_clusters"
    conditions = []
    params = {}
    conditions.append("oday >= %(from_oday)s::date")
    conditions.append("oday <= %(to_oday)s::date")
    params["from_oday"] = from_oday
    params["to_oday"] = to_oday

    if route_ids:
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

async def load_compressed_departures_csv(route_ids: [str], from_oday: str, to_oday: str) -> bytes:
    base_query = "SELECT zst FROM delay.preprocess_departures"
    conditions = []
    params = {}
    conditions.append("oday >= %(from_oday)s::date")
    conditions.append("oday <= %(to_oday)s::date")
    params["from_oday"] = from_oday
    params["to_oday"] = to_oday

    if route_ids:
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

    excluded_odays = DAYS_TO_EXCLUDE
    if excluded_odays:
        combined_df = combined_df[~combined_df["oday"].isin(excluded_odays)]

    if combined_df.empty:
        return None

    buffer = io.BytesIO()
    combined_df.to_csv(buffer, sep=";", index=False)
    buffer.seek(0)
    return buffer.getvalue()


async def load_compressed_cluster(table: str, route_id: str, from_oday: str, to_oday: str) -> bytes:
    table_name = f"delay.{table}"
    query = f"""
        SELECT zst
        FROM {table_name}
        WHERE route_id = %(route_id)s AND from_oday >= %(from_oday)s AND to_oday <= %(to_oday)s
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
    decompressed_csv = dctx.decompress(compressed_data)
    return decompressed_csv 

async def prep_cluster_data(
    routes_table: str,
    modes_table: str,
    route_ids: Union[str, list[str]],
    from_oday: date,
    to_oday: date
) -> tuple[Optional[bytes], Optional[bytes]]:

    routecluster_geojson = await load_compressed_cluster(routes_table, route_ids, from_oday, to_oday)
    modecluster_geojson  = await load_compressed_cluster(modes_table,  route_ids, from_oday, to_oday)

    if route_ids != "ALL" and (routecluster_geojson is None or modecluster_geojson is None):
        await recluster_analysis(route_ids, from_oday, to_oday)
        routecluster_geojson = await load_compressed_cluster(routes_table, route_ids, from_oday, to_oday)
        modecluster_geojson  = await load_compressed_cluster(modes_table,  route_ids, from_oday, to_oday)

    return routecluster_geojson, modecluster_geojson

async def store_compressed_geojson(
    table: str,
    route_id: str,
    from_oday: str,
    to_oday: str,
    gdf: gpd.GeoDataFrame
):
    """
    Convert the GeoDataFrame to GeoJSON and compress with zstd
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
            SET zst = EXCLUDED.zst
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
    min_weighted_samples: int,
    vars_to_group_level_one_clusters_by=['route_id', 'direction_cluster_id', 'time_group', 'dclass'],
    cluster_id_vars_on_2nd_level=['route_id', 'direction_id', 'time_group', 'dclass', 'cluster_on_reclustered_level'],
) -> pd.DataFrame:

    g = clusters.groupby(vars_to_group_level_one_clusters_by)

    departure_clusters = []
    reclustered_clusters = []
    EPSILON = distance / EARHT_RADIUS_KM
    
    for k, sub in g:
        sub = sub.rename(columns={'cluster': 'cluster_on_departure_level'})
        # not used?
        # X = np.radians(sub[['lat_median', 'long_median']])

        clusterer = DBSCAN(
            eps=EPSILON,
            min_samples=min_weighted_samples,
            metric='haversine'
        )

        # weight on sekuntien lkm ensimmäisen tason klusteroinnista: mitä enemmän lähtö kärsii viiveestä, sen suuremman painoarvon se saa klusterissa
        sub['cluster_on_reclustered_level'] = clusterer.fit_predict(sub[['lat_median', 'long_median']], sample_weight=sub['weight'])
        sub = sub[sub['cluster_on_reclustered_level'] != -1]  # Remove noise points

        if sub.empty:
            continue

        departure_clusters.append(sub)
        sub = calculate_cluster_features(sub, cluster_id_vars_on_2nd_level)
        reclustered_clusters.append(sub)

    departure_clusters = pd.concat(departure_clusters)
    reclustered_clusters = pd.concat(reclustered_clusters)

    return reclustered_clusters, departure_clusters


def calculate_cluster_features(df: pd.DataFrame, cluster_id_vars_on_2nd_level: list) -> pd.DataFrame:
    df["start"] = pd.to_datetime(df["start"], format="ISO8601")
    df["tst_median"] = pd.to_datetime(df["tst_median"], format="ISO8601")
    df["oday"] = pd.to_datetime(df["oday"])

    clust_counts = df.drop_duplicates(subset=['route_id', 'direction_id', 'oday', 'start', 'cluster_on_reclustered_level'])
    clust_counts = clust_counts.groupby(cluster_id_vars_on_2nd_level).size().reset_index(name='departures')
    clust_delay_feats = df.groupby(cluster_id_vars_on_2nd_level)['weight'].quantile([0.10, 0.25, 0.5, 0.75, 0.90]).unstack().add_prefix('q_').reset_index()
    
    median_vars = df.groupby(cluster_id_vars_on_2nd_level)[['lat_median', 'long_median', 'tst_median', 'hdg_median']].median().reset_index()
    df = median_vars.merge(clust_counts, on=cluster_id_vars_on_2nd_level, how='outer')
    df = df.merge(clust_delay_feats, on=cluster_id_vars_on_2nd_level, how='outer')

    return df


def ui_related_var_modifications(df):
    # NOTE nämä muuttujakäsittelyt yms REMIX-spesifit asiat voisi tehdä HFP API:n rajapinnan jälkeen ellei

    df['year'] = df['tst_median'].dt.year
    df['season'] = get_season(df.loc[0, "tst_median"])

    df['quantile_ratio_q75_q50'] = df['q_0.75'] / df['q_0.5']

    for k, v in DCLASS_NAMES.items():
        df['dclass'] = df['dclass'].replace(k, v)

    # mediaanin luokat avoimella ylärajalla
    df['q_05._category'] = np.where(df['q_0.5'] <= 30, '0_15_30', '>75')
    df['q_05._category'] = np.where((df['q_0.5'] > 30) & (df['q_0.5'] <= 45), '1_30_45', df['q_05._category'])
    df['q_05._category'] = np.where((df['q_0.5'] > 45) & (df['q_0.5'] <= 60), '2_45_60', df['q_05._category'])
    df['q_05._category'] = np.where((df['q_0.5'] > 60) & (df['q_0.5'] <= 75), '3_60_74', df['q_05._category'])
    return df


async def get_preprocessed_departures(route_ids: [str], from_oday: str, to_oday: str):
    departures_data = await load_compressed_departures_csv(route_ids, from_oday, to_oday)
    if not departures_data:
        print(f"No departures ZST found for route_id={route_ids}")
        return None

    preprocessed_departures = pd.read_csv(io.BytesIO(departures_data), sep=';')

    return preprocessed_departures

async def get_preprocessed_clusters(route_ids: [str], from_oday: str, to_oday: str):
    cluster_data = await load_compressed_cluster_csv(route_ids, from_oday, to_oday)
    if not cluster_data:
        print(f"No cluster ZST found for route_id={route_ids}")
        return None

    clusters = pd.read_csv(io.BytesIO(cluster_data), sep=";")

    week_days_df = clusters[
        clusters["time_group"].str.contains("weekday", case=False, na=False)
    ].copy()
    week_days_df["time_group"] = "0_weekday_all"

    clusters = pd.concat([clusters, week_days_df], axis=0).reset_index(drop=True)
    return clusters


async def recluster_analysis(route_ids: [str], from_oday: str, to_oday: str):
    clusters = await get_preprocessed_clusters(route_ids, from_oday, to_oday)
    preprocessed_departures = await get_preprocessed_departures(route_ids, from_oday, to_oday)

    if clusters is None or preprocessed_departures is None:
        return

    num_of_deps_analyzed = preprocessed_departures.groupby(['route_id', 'direction_id', 'time_group']).size().to_frame().reset_index().rename(columns={0: 'num_of_deps_analyzed'})
    route_clusters, departure_clusters = recluster(
        clusters,
        distance=EPS_DISTANCE_2,
        min_weighted_samples=MIN_WEIGHTED_SAMPLES,
        vars_to_group_level_one_clusters_by=['route_id', 'direction_id', 'time_group', 'dclass'],
        cluster_id_vars_on_2nd_level=['route_id', 'direction_id', 'time_group', 'dclass', 'cluster_on_reclustered_level']
    )

    route_clusters = route_clusters[route_clusters["q_0.5"] >= MIN_MEDIAN_DELAY_IN_CLUSTER]
    route_clusters = route_clusters.merge(num_of_deps_analyzed, how='left', on=['route_id', 'direction_id', 'time_group'])
    route_clusters['share_of_departures'] = route_clusters['departures'] / route_clusters['num_of_deps_analyzed'] * 100

    departure_clusters = route_clusters[["route_id", "direction_id", "time_group", "dclass", "cluster_on_reclustered_level"]].merge(
        departure_clusters, on=["route_id", "direction_id", "time_group", "dclass", "cluster_on_reclustered_level"], how="inner"
    )

    route_clusters = ui_related_var_modifications(route_clusters)

    route_clusters['route_dir'] = route_clusters['route_id'].astype(str) + " S" + route_clusters['direction_id'].astype(str)
    bins = list(range(0, 101, 10))
    route_clusters['shares_category'] = pd.cut(
        route_clusters['share_of_departures'], bins=bins, labels=["0_10", "10_20", "20_30", "30_40", "40_50", "50_60", "60_70", "70_80", "80_90", "90_100"], include_lowest=True
    )
    route_clusters["shares_category"] = route_clusters["shares_category"].astype(str)
    route_clusters['share_of_departures'] = round(route_clusters['share_of_departures'], 1)
    # df = df[df['share_of_departures'] > 0.0].reset_index(drop=True)
    route_clusters = route_clusters.drop('cluster_on_reclustered_level', axis=1)

    route_clusters = make_geo_df_WGS84(route_clusters, lat_col="lat_median", lon_col="long_median", crs="EPSG:4326")  # .drop(['lat_median', 'long_median'], axis=1)
    
    db_route_id = route_ids
    if not db_route_id:
        db_route_id = 'ALL'

    # Is there a reason to store this in db and not just return it as response?
    await store_compressed_geojson("recluster_routes", db_route_id, from_oday, to_oday, route_clusters)
    
    #assert route_clusters['share_of_departures'].max() <= 100
    #assert route_clusters[route_clusters.duplicated()].empty

    clusters = clusters.merge(preprocessed_departures[['route_id', 'direction_id', 'oday', 'start', 'transport_mode']], how="left", on=['route_id', 'direction_id', 'oday', 'start'])
    num_of_deps_analyzed = clusters.groupby(["transport_mode", "time_group"]).size().to_frame().reset_index().rename(columns={0: 'num_of_deps_analyzed'})

    mode_clusters, departure_clusters = recluster(
        clusters,
        distance=EPS_DISTANCE_2,
        min_weighted_samples=MIN_WEIGHTED_SAMPLES,
        vars_to_group_level_one_clusters_by=["transport_mode", 'time_group', 'dclass'],
        cluster_id_vars_on_2nd_level=["transport_mode", 'time_group', 'dclass', 'cluster_on_reclustered_level']
    )

    mode_clusters = mode_clusters[mode_clusters["q_0.5"] >= MIN_MEDIAN_DELAY_IN_CLUSTER]
    mode_clusters = mode_clusters.merge(num_of_deps_analyzed, how='left', on=['transport_mode', 'time_group'])

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
        departure_clusters['m_norm_hdg_median'] = departure_clusters.groupby(['dclass', 'cluster_on_reclustered_level', 'time_group', 'transport_mode'])['hdg_median'].transform(
            lambda x: (x - x.median()) / (x.quantile(0.75) - x.quantile(0.25))
        )
   
    # var reprocessing
    mode_clusters = ui_related_var_modifications(mode_clusters)
    mode_clusters['transport_mode'] = mode_clusters['transport_mode'].replace('bus', 'Bussi').replace('tram', 'Raitiovaunu')
    # mode_clusters['share_of_departures'] = mode_clusters['departures'] / mode_clusters['num_of_deps_analyzed'] * 100 # NOTE: This var is redundant ATM
    mode_clusters = mode_clusters.drop('cluster_on_reclustered_level', axis=1)
    mode_clusters = make_geo_df_WGS84(mode_clusters, lat_col="lat_median", lon_col="long_median", crs="EPSG:4326")  # .drop(['lat_median', 'long_median'], axis=1)

    # Is there a reason to store this in db and not just return it as response?
    await store_compressed_geojson("recluster_modes", db_route_id, from_oday, to_oday, mode_clusters)
