import io
import gzip
import pandas as pd
import pytz
import numpy as np
import zstandard as zstd
import psycopg
import logging
from common.database import pool
from common.container_client import FlowAnalyticsContainerClient
from sklearn.cluster import DBSCAN
from collections import Counter
from typing import Optional
from datetime import date, timedelta, datetime, time
from io import BytesIO

from common.utils import get_previous_day_oday

logger = logging.getLogger("analyzer")

# TODO: Move to configs
EARHT_RADIUS_KM = 6371
EPS_DISTANCE_1 = 0.01
MIN_DELAY_EVENTS = 5
HDG_DIFF_LOWER_LIMIT = 170
HDG_DIFF_UPPER_LIMIT = 190
HDG_DIFF_LOWER_LIMIT = 170
HDG_DIFF_UPPER_LIMIT = 190
HDG_SPEED_LIMIT = 2.0
MAX_TIME_GAP = 60


SPEEDS_IN_DELAY = ['DELAY', 'SLOW']
SPEED_CLASSES = {
    "DELAY": {
        "DELAY_MAX": 0.27,
    },
    "SLOW": {
        "SLOW_MIN": 0.27,
        "SLOW_MAX": 0.83,
    },
    "FAST": {
        "FAST_MIN": 0.83,
    },
}

TIME_GROUP_D = {
    "AHT": {
        "START": "6:30",
        "END": "9:00"
    },
    "PT": {
        "START": "9:00",
        "END": "15:00"
    },
    "IHT": {
        "START": "15:00",
        "END": "18:00"
    }
}

async def load_delay_hfp_data(route_id: Optional[str], oday: date) -> pd.DataFrame:
    csv_buffer = io.BytesIO()
    row_count = await get_delay_hfp_data(route_id, oday, csv_buffer)

    csv_buffer.seek(0)

    df = pd.read_csv(csv_buffer)
    return df

async def get_delay_hfp_data(
    route_id: Optional[str],
    oday: date,
    stream: BytesIO,
) -> int:
    """
    Query delay hfp data.
    """
    oday_datetime = datetime.strptime(oday, "%Y-%m-%d").date()

    from_datetime = datetime.combine(oday_datetime, time(0, 0, 0))
    to_datetime = from_datetime + timedelta(days=1, hours=1)

    from_tst = from_datetime.isoformat()
    to_tst = to_datetime.isoformat()

    query = f"""
        COPY (
            SELECT
                *
            FROM api.view_as_original_hfp_event
            WHERE
                (%(route_id)s IS NULL OR route_id = %(route_id)s) AND
                oday = %(oday)s AND tst >= %(from_tst)s AND tst <= %(to_tst)s
        ) TO STDOUT WITH CSV HEADER
    """


    async with pool.connection() as conn:
        async with conn.cursor().copy(
            query,
            {
                "route_id": route_id,
                "oday": oday,
                "from_tst": from_tst,
                "to_tst": to_tst
            },
        ) as copy:
            row_count = -1

            async for row in copy:
                row_count += 1
                stream.write(row)
        return row_count

def tst_seconds_from_midnight(df):
    """
    From the HFP event timestamp calculate how many seconds from midnight the event
    took place. Optional measure for time elapsed for timestamps.
    """
    df["tst_seconds_from_midnight"] = df.tst.dt.hour * 60 * 60 + df.tst.dt.minute * 60 + df.tst.dt.second
    return df

def make_time_groups(df, column_with_localized_date, column_with_localized_time, time_group_d):
    """Make time groups to aggregate or classify data based on timestamps.

    Args:
        df (pd.DataFrame): df with date and time
        column_with_localized_date (str): column with date
        column_with_localized_time (datetime64[ns, Europe/Helsinki]): datetime column from which the time will be extracted


    Returns:
        pd.DataFrame column: column with the time group values.
    """

    res = pd.DataFrame(pd.to_datetime(df[column_with_localized_date]))

    res["day_type_of_oday"] = np.where(res[column_with_localized_date].dt.dayofweek.isin([5, 6]), "weekend", "weekday")
    df[column_with_localized_time] = pd.to_datetime(
        df[column_with_localized_time],
        format="%H:%M:%S",
        errors="coerce"
    )
    res["tst_time"] = df[column_with_localized_time].dt.time
    res["time_group"] = "weekday_other"

    # TODO: Check if the comparison working here?
    for k, v in time_group_d.items():
        res["time_group"] = np.where(
            (res.day_type_of_oday == "weekday")
            & (res["tst_time"] >= datetime.strptime(v['START'], "%H:%M").time())
            & (res["tst_time"] < datetime.strptime(v["END"], "%H:%M").time()),
            f'{k}_{v["START"]}_{v["END"]}',
            res["time_group"],
        )

    res["time_group"] = res["time_group"].replace("weekday_other", f"{len(time_group_d.keys())+1}_weekday_other")

    res["time_group"] = np.where(
        res.day_type_of_oday == "weekend",
        f"{len(time_group_d.keys())+2}_weekend",
        res["time_group"],
    )

    return res["time_group"]


def compress_csv_bytes_to_zst(csv_bytes: bytes) -> bytes:
    """Compress raw CSV bytes to zstd."""
    import zstandard as zstd
    cctx = zstd.ZstdCompressor()
    return cctx.compress(csv_bytes)

async def store_compressed_csv(
    table: str,
    route_id: str,
    mode: str,
    oday: str,
    df: pd.DataFrame,
    flow_analytics_container_client: FlowAnalyticsContainerClient,
):
    """
    Store df as a compressed CSV into the database table "schema.table".
    """
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, sep=";", encoding="utf-8", index=False)
    csv_buffer.seek(0)
    csv_bytes = csv_buffer.getvalue()

    compressed_csv = compress_csv_bytes_to_zst(csv_bytes)

    table_full_name = f"delay.{table}"
    query = f"""
        INSERT INTO {table_full_name} (route_id, mode, oday, zst)
        VALUES (%(route_id)s, %(mode)s, %(oday)s, %(zst)s)
        ON CONFLICT (route_id, oday) DO UPDATE
            SET oday      = EXCLUDED.oday,
                zst       = EXCLUDED.zst
    """

    async with pool.connection() as conn:
        await conn.execute(
            query,
            {
                "route_id": route_id,
                "mode": mode,
                "oday": oday,
                "zst": compressed_csv,
            },
        )
    
    preprocess_type = table.split('_')[1]
    
    await flow_analytics_container_client.save_preprocess_data(
        preprocess_type= preprocess_type,
        compressed_csv=compressed_csv, 
        route_id=route_id, 
        mode=mode, 
        oday=oday
    )

async def check_preprocessed_files(route_id: str, oday: date, table: str) -> bool:
    """
    Return true of preprocessed file is found with given params.
    """
    table_name = f"delay.{table}"
    query = f"""
        SELECT count(*)
        FROM {table_name}
        WHERE oday = %(oday)s
          AND route_id = %(route_id)s
    """
    async with pool.connection() as conn:
        result_cursor = await conn.execute(query, {"route_id": route_id, "oday": oday})
        row = await result_cursor.fetchone()
        return row and row[0] > 0

async def preprocess(
    df: pd.DataFrame,
    route_id: str,
    oday: str,
):

    clusters = []  # tämä on aggregoinnin tason 1 output!,
    departures = []  # tämä on aggregoinnin tason 1 output!,

    speed_in_location = []
    failed_in_quality = []
    vp_events_in_clusters = []
    timezone = pytz.timezone("Europe/Helsinki")
    counts = Counter(df.oday)
    file_date = None
    if counts:
        file_date = counts.most_common()[0][0]
    else:
        raise ValueError("No oday found. Skipping")

    counts = Counter(df.transport_mode)
    mode = ""
    if counts:
        mode = counts.most_common()[0][0]
    else:
        logger.debug("No transport_mode found.")

    df["oday"] = pd.to_datetime(df.oday)
    df['stop'] = df['stop'].astype('Int64')
    df.tst = pd.to_datetime(df.tst, format="ISO8601").dt.tz_convert(timezone)
    df['time_group'] = make_time_groups(df, "oday", "start", TIME_GROUP_D)
    # for every departure:
    g = df.groupby(
        [
            "route_id",
            "direction_id",
            "oday",
            "start",
            "vehicle_number",
        ]
    )  # veh number should not s_and_d_s_and_d_s_and_d_change during a departure but just to be careful keep it in!,
    for key, sub_df in g:
        sub_df = sub_df.reset_index(drop=True)
        
        sub_df = sub_df[sub_df["loc"].isin(["GPS", "DR"])].reset_index(drop=True)
        if sub_df.empty:
            continue

        vp_event_df = sub_df[(sub_df["event_type"] == "VP")]
        if vp_event_df.empty:
            continue

        vp_event_df = vp_event_df.drop_duplicates().sort_values(by="tst").reset_index(drop=True)
        # Helper variables
        vp_event_df = tst_seconds_from_midnight(vp_event_df)
        vp_event_df["diff_btwn_tsts"] = np.append([0], np.diff(vp_event_df["tst_seconds_from_midnight"]))
        vp_event_df["diff_btwn_tsts"] = np.where(vp_event_df["diff_btwn_tsts"] == -60 * 60 * 24 + 1, 1, vp_event_df["diff_btwn_tsts"])
        vp_event_df["hdg"] = vp_event_df["hdg"].ffill().bfill()
        vp_event_df["diff_btwn_hdg"] = np.append([0], np.diff(vp_event_df["hdg"]))
        vp_event_df = vp_event_df.reset_index(drop=True)

        if (vp_event_df["drst"] == 0.0).all():
            vp_event_df['ERR_all_door_status_closed'] = True
        if (vp_event_df["drst"] == 1.0).all():
            vp_event_df['ERR_all_door_status_open'] = True
        if vp_event_df['drst'].isna().all():
            vp_event_df['ERR_all_door_status_NA'] = True
        if len(vp_event_df) <= 5:
            vp_event_df['ERR_too_few_VP'] = True
        if vp_event_df['stop'].isna().all():
            vp_event_df['ERR_no_route_stops'] = True
        if pd.isna(vp_event_df["odo"]).all():
            vp_event_df['ERR_all_odo_NA'] = True
        if pd.isna(vp_event_df["lat"]).all():
            vp_event_df['ERR_all_lat_NA'] = True
        if pd.isna(vp_event_df["long"]).all():
            vp_event_df['ERR_all_long_NA'] = True
        if (vp_event_df["lat"] == 0.0).all():
            vp_event_df['ERR_all_lat_0'] = True
        if (vp_event_df["long"] == 0.0).all():
            vp_event_df['ERR_all_long_0'] = True
        if any(vp_event_df.diff_btwn_tsts > MAX_TIME_GAP):
            vp_event_df['ERR_too_long_time_gap'] = True
        if any((np.abs(vp_event_df.diff_btwn_hdg).between(HDG_DIFF_LOWER_LIMIT, HDG_DIFF_UPPER_LIMIT, inclusive='both')) & (vp_event_df.spd > HDG_SPEED_LIMIT)):
            vp_event_df['ERR_heading_diff_error'] = True
        if [x for x in vp_event_df.columns if x.startswith('ERR')]:
            failed_in_quality.append(vp_event_df)
            continue

        # Remove data before first stop and after last stop
        vp_event_df = vp_event_df.loc[vp_event_df[pd.notna(vp_event_df.stop)].head(1).index.values[0] :,].reset_index(drop=True)
        if vp_event_df.empty:
            continue
        vp_event_df = vp_event_df.loc[: vp_event_df[pd.notna(vp_event_df.stop)].tail(1).index.values[0],].reset_index(drop=True)
        if vp_event_df.empty:
            continue

        departures.append(
            vp_event_df.head(1).reset_index(drop=True)[['tst', 'event_type', 'route_id', 'direction_id', 'operator_id', 'oper', 'vehicle_number', 'transport_mode', 'oday', 'start', 'time_group']]
        )

        vp_event_df = vp_event_df.dropna(subset=["lat", "long"], how="all")
        vp_event_df = vp_event_df.sort_values(by="tst").reset_index(drop=True)
        vp_event_df["odo_spd"] = np.append(np.nan, np.diff(vp_event_df["odo"]))
        vp_event_df["odo_spd"] = vp_event_df["odo_spd"].rolling(window=5, min_periods=1).mean()
        vp_event_df["odo_spd"] = vp_event_df["odo_spd"] / vp_event_df["diff_btwn_tsts"]
        vp_event_df["odo_spd"] = vp_event_df["odo_spd"].ffill().bfill()
        vp_event_df["mean_spd"] = vp_event_df[["odo_spd", "spd"]].mean(axis=1)

        # Speed classes
        vp_event_df["drst"] = vp_event_df["drst"].fillna(2)
        vp_event_df["sclass"] = ""
        doors_closed = vp_event_df[vp_event_df["drst"] == 0]

        for k, v in SPEED_CLASSES.items():
            if "SLOW_MIN" in v:
                doors_closed.loc[doors_closed["mean_spd"].between(v["SLOW_MIN"], v["SLOW_MAX"]), "sclass"] = k
            elif "DELAY_MAX" in v:
                doors_closed.loc[doors_closed["mean_spd"] < v["DELAY_MAX"], "sclass"] = k
            else:
                doors_closed.loc[doors_closed["mean_spd"] > v["FAST_MIN"], "sclass"] = k

        doors_opened = vp_event_df[vp_event_df["drst"] != 0]
        doors_opened.loc[doors_opened["drst"] == 2, "sclass"] = "DRS_ERR"
        doors_opened.loc[doors_opened["mean_spd"] < 1, "sclass"] = "STOP"
        doors_opened.loc[doors_opened["mean_spd"] >= 1, "sclass"] = "SPD_ERR"
        
        # Delay classes
        vp_event_df = pd.concat([doors_closed, doors_opened], axis=0).sort_values(by="tst").reset_index(drop=True)
        vp_event_df['stop'] = vp_event_df['stop'].fillna(0)
        g = vp_event_df.groupby('stop')
        dfs = []
        for k, res in g:
            if k == 0:
                res['dclass'] = 'on_route'
            elif set(res.drst) == {1, 0}:
                idx_doors_open_first_time = res[res.drst == 1].sort_values(by='tst').head(1).index.values[0]
                # NOTE ajantasauksissa seisotaan paikoillaan ovet kiinni, niiden käsittely olisi oma hommansa, jota ei ole nyt huomioitu koodissa
                res['dclass'] = np.where((res.index < idx_doors_open_first_time) & (pd.notna(res.stop)), "arr", "stop")
                idx_doors_close_last_time = res[res.drst == 1].sort_values(by='tst').tail(1).index.values[0]
                res['dclass'] = np.where((res.index > idx_doors_close_last_time) & (pd.notna(res.stop)), "dep", res['dclass'])
            else:
                res['dclass'] = np.where(pd.notna(res.stop), "pass", "")
            dfs.append(res)

        dfs = pd.concat(dfs)
        # NOTE: pysäkillä seisominen käsitetään matkustajapalveluksi tai pysäkkiajaksi, ei viiveeksi
        dfs = dfs[dfs.dclass != "stop"]
        vp_event_df = dfs.sort_values(by='tst').reset_index(drop=True)
        # speed_in_location.append(vp_event_df)
        delay_df = vp_event_df[vp_event_df['sclass'].isin(['DELAY', 'SLOW'])].copy()

        # aggregation level 1
        if not delay_df.empty:
            groups = delay_df.groupby('dclass')
            for _, delay_class_df in groups:
                # vp_events_in_clusters.append(delay_class_df)
                EPSILON = EPS_DISTANCE_1 / EARHT_RADIUS_KM
                X = np.radians(delay_class_df[['lat', 'long']])
                dbscan = DBSCAN(eps=EPSILON, min_samples=MIN_DELAY_EVENTS, metric='haversine')
                delay_class_df['cluster'] = dbscan.fit_predict(X)
                delay_class_df = delay_class_df[delay_class_df['cluster'] != -1]
            
                if not delay_class_df.empty:
                    my_vars = ['route_id', 'direction_id', 'dclass', 'oday', 'start', 'time_group', 'cluster']
                    cluster_counts = delay_class_df.groupby(my_vars).size().reset_index(name='weight')
                    median_vars = delay_class_df.groupby(my_vars)[['lat', 'long', 'tst', 'hdg']].median().reset_index()
                    cluster_df = median_vars.merge(cluster_counts, on=my_vars, how='outer')
                    cluster_df = cluster_df[['route_id', 'direction_id', 'hdg', 'dclass', 'oday', 'start', 'tst', 'weight', 'time_group', 'lat', 'long']].rename(
                        columns={'tst': 'tst_median', 'hdg': 'hdg_median', 'lat': 'lat_median', 'long': 'long_median'}
                    )
                    clusters.append(cluster_df)
    

    flow_analytics_container_client = FlowAnalyticsContainerClient()
    
    if clusters:
        clusters_df = pd.concat(clusters)
        await store_compressed_csv(
            "preprocess_clusters",
            route_id,
            mode,
            oday,
            clusters_df,
            flow_analytics_container_client=flow_analytics_container_client,
        )
    if departures:
        departures_df = pd.concat(departures)
        await store_compressed_csv(
            "preprocess_departures",
            route_id,
            mode,
            oday,
            departures_df,
            flow_analytics_container_client=flow_analytics_container_client,
        )
    if vp_events_in_clusters:
        path = f"./HFP_vp_events_in_clusters_{str(key[0])}_{file_date}.csv"
        #pd.concat(vp_events_in_clusters).to_csv(path, sep=";", encoding="utf-8", index=False)
    if failed_in_quality:
        path = f"./failure_debug_data_{str(key[0])}_{file_date}.csv"
        #pd.concat(failed_in_quality).to_csv(path, sep=";", encoding="utf-8", index=False)
    # path = f"./auxillary/failed_in_quality/failure_debug_data_{k[0]}_{file_date}.csv"
    # pd.concat(failed_in_quality).to_csv(path, sep=";", encoding="utf-8", index=False)"
