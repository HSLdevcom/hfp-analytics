CREATE SCHEMA staging;
COMMENT ON SCHEMA staging IS 'Schema containing temporal data to be imported.';


CREATE TABLE staging.hfp_raw (
  tst                   timestamptz   NOT NULL,
  event_type            text          NOT NULL,
  received_at           timestamptz,
  vehicle_operator_id   smallint      NOT NULL,
  vehicle_number        integer       NOT NULL,
  transport_mode        text,
  route_id              text,
  direction_id          smallint,
  oday                  date,
  "start"               interval,
  observed_operator_id  smallint,
  odo                   real,
  spd                   real,
  drst                  bool,
  loc                   text,
  stop                  integer,
  longitude             double precision,
  latitude              double precision
);
COMMENT ON TABLE staging.hfp_raw IS 'Table where the client copies hfp data to be imported to hfp schema.';


CREATE OR REPLACE PROCEDURE staging.remove_accidental_signins()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM hfp.assumed_monitored_vehicle_journey
    WHERE age(max_timestamp, min_timestamp) < interval '1 minute';
END;
$$;

CREATE OR REPLACE PROCEDURE staging.import_and_normalize_hfp()
LANGUAGE sql
AS $procedure$
  INSERT INTO hfp.hfp_point (
    point_timestamp,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    hfp_event,
    received_at,
    odo,
    spd,
    drst,
    loc,
    stop,
    geom
  )
  SELECT
    tst,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    event_type,
    received_at,
    odo,
    spd,
    drst,
    loc,
    stop,
    ST_Transform( ST_SetSRID( ST_MakePoint(longitude, latitude), 4326), 3067)
  FROM staging.hfp_raw
  -- Ordering is here for a reason. It makes data clustered inside a blob so querying by route / vehicle is more efficient.
  ORDER BY route_id, vehicle_number
  ON CONFLICT DO NOTHING;

  INSERT INTO hfp.assumed_monitored_vehicle_journey (
    vehicle_operator_id, vehicle_number, transport_mode, route_id, direction_id, oday, "start", observed_operator_id, min_timestamp, max_timestamp, arr_count
  )
  SELECT
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    min(tst) AS min_timestamp,
    max(tst) AS max_timestamp,
    SUM(CASE WHEN event_type = 'ARR' THEN 1 ELSE 0 END) AS arr_count
  -- (Add further aggregates such as N of hfp_point rows here, if required later.
  -- Be careful about min_tst, because aggregate might not give all records, if there were ones before min_tst.
  FROM staging.hfp_raw
  WHERE
    vehicle_operator_id != '0199' AND
    transport_mode IS NOT NULL AND
    route_id IS NOT NULL AND
    direction_id IS NOT NULL AND
    oday IS NOT NULL AND
    "start" IS NOT NULL AND
    observed_operator_id IS NOT NULL
  GROUP BY
    vehicle_operator_id, vehicle_number, transport_mode, route_id, direction_id, oday, "start", observed_operator_id
  -- Update existing rows in target table by (vehicle_id, journey_id),
  -- update min and max timestamps as we might get new values for them
  -- when importing hfp data to fill a gap or if more recent data is available
  -- when running import.
  ON CONFLICT ON CONSTRAINT assumed_monitored_vehicle_journey_pkey DO UPDATE SET
    max_timestamp = greatest(assumed_monitored_vehicle_journey.max_timestamp, EXCLUDED.max_timestamp),
    min_timestamp = least(assumed_monitored_vehicle_journey.min_timestamp, EXCLUDED.min_timestamp),
    arr_count = assumed_monitored_vehicle_journey.arr_count + EXCLUDED.arr_count,
    modified_at = now()
  WHERE
  -- Update only if values are actually changed, so that modified_at -field shows the correct time.
    assumed_monitored_vehicle_journey.min_timestamp != EXCLUDED.min_timestamp OR
    assumed_monitored_vehicle_journey.max_timestamp != EXCLUDED.max_timestamp OR
    (assumed_monitored_vehicle_journey.arr_count + EXCLUDED.arr_count) != assumed_monitored_vehicle_journey.arr_count;
$procedure$;

COMMENT ON PROCEDURE staging.import_and_normalize_hfp IS 'Procedure to copy data from staging schema to hfp schema.';


CREATE OR REPLACE PROCEDURE staging.import_invalid_hfp()
LANGUAGE sql
AS $procedure$
  INSERT INTO hfp.hfp_point_invalid (
    point_timestamp,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    hfp_event,
    received_at,
    odo,
    spd,
    drst,
    loc,
    stop,
    geom
  )
  SELECT
    tst,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    event_type,
    received_at,
    odo,
    spd,
    drst,
    loc,
    stop,
    ST_Transform( ST_SetSRID( ST_MakePoint(longitude, latitude), 4326), 3067)
  FROM staging.hfp_raw
  ON CONFLICT DO NOTHING;
$procedure$;

COMMENT ON PROCEDURE staging.import_invalid_hfp IS 'Procedure to copy data marked as invalid from staging schema to hfp schema.';


CREATE TABLE staging.apc_raw (
  point_timestamp       timestamptz   NOT NULL,
  received_at           timestamptz,
  vehicle_operator_id   smallint      NOT NULL,
  vehicle_number        integer       NOT NULL,
  transport_mode        text,
  route_id              text,
  direction_id          smallint,
  oday                  date,
  "start"               interval,
  observed_operator_id  smallint,
  stop                  integer,
  vehicle_load          integer,
  vehicle_load_ratio    real,
  doors_data            jsonb,
  count_quality         text,
  longitude             double precision,
  latitude              double precision
);

CREATE OR REPLACE PROCEDURE staging.import_and_normalize_apc()
LANGUAGE sql
AS $procedure$
  INSERT INTO apc.apc (
    point_timestamp,
    received_at,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    stop,
    vehicle_load,
    vehicle_load_ratio,
    doors_data,
    count_quality,
    geom
  )
  SELECT
    point_timestamp,
    received_at,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    "start",
    observed_operator_id,
    stop,
    vehicle_load,
    vehicle_load_ratio,
    doors_data,
    count_quality,
    ST_Transform( ST_SetSRID( ST_MakePoint(longitude, latitude), 4326), 3067)
  FROM staging.apc_raw
  -- Ordering is here for a reason. It makes data clustered inside a blob so querying by route / vehicle is more efficient.
  ORDER BY route_id, vehicle_number
  ON CONFLICT DO NOTHING;
$procedure$;

COMMENT ON PROCEDURE staging.import_and_normalize_apc IS 'Procedure to copy data from staging schema to apc schema.';

CREATE TABLE staging.tlr_raw (
  acc                   real,
  desi                  text,
  dir                   integer,
  direction_id          integer,
  dl                    integer,
  dr_type               text,
  drst                  boolean,
  event_type            text,
  geohash_level         integer,
  hdg                   integer,
  headsign              text,
  is_ongoing            boolean,
  journey_start_time    text,
  journey_type          text,
  jrn                   integer,
  latitude              double precision,
  line                  text,
  location_quality_method text,
  longitude             double precision,
  mode                  text,
  next_stop_id          text,
  occu                  integer,
  oday                  date,
  odo                   real,
  oper                  integer,
  owner_operator_id     integer,
  received_at           timestamptz,
  route                 text,
  route_id              text,
  seq                   text,
  sid                   integer,
  signal_group_id       integer,
  spd                   real,
  start                 text,
  stop                  text,
  tlp_att_seq           integer,
  tlp_decision          text,
  tlp_frequency         text,
  tlp_line_config_id    text,
  tlp_point_config_id   text,
  tlp_priority_level    text,
  tlp_protocol          text,
  tlp_reason            text,
  tlp_request_id        text,
  tlp_request_type      text,
  tlp_signal_group_nbr  integer,
  topic_latitude        double precision,
  topic_longitude       double precision,
  topic_prefix          text,
  topic_version         text,
  tsi                   bigint,
  point_timestamp       timestamptz,
  unique_vehicle_id     text,
  uuid                  uuid,
  veh                   integer,
  vehicle_number        integer NOT NULL
);


CREATE OR REPLACE PROCEDURE staging.import_and_normalize_tlr()
LANGUAGE sql
AS $procedure$
  INSERT INTO tlr.tlr (
    acc,
    desi,
    dir,
    direction_id,
    dl,
    dr_type,
    drst,
    event_type,
    geohash_level,
    hdg,
    headsign,
    is_ongoing,
    journey_start_time,
    journey_type,
    jrn,
    latitude,
    line,
    location_quality_method,
    longitude,
    mode,
    next_stop_id,
    occu,
    oday,
    odo,
    oper,
    owner_operator_id,
    received_at,
    route,
    route_id,
    seq,
    sid,
    signal_group_id,
    spd,
    start,
    stop,
    tlp_att_seq,
    tlp_decision,
    tlp_frequency,
    tlp_line_config_id,
    tlp_point_config_id,
    tlp_priority_level,
    tlp_protocol,
    tlp_reason,
    tlp_request_id,
    tlp_request_type,
    tlp_signal_group_nbr,
    topic_latitude,
    topic_longitude,
    topic_prefix,
    topic_version,
    tsi,
    point_timestamp,
    unique_vehicle_id,
    uuid,
    veh,
    vehicle_number
  )
  SELECT
    acc,
    desi,
    dir,
    direction_id,
    dl,
    dr_type,
    drst,
    event_type,
    geohash_level,
    hdg,
    headsign,
    is_ongoing,
    journey_start_time,
    journey_type,
    jrn,
    latitude,
    line,
    location_quality_method,
    longitude,
    mode,
    next_stop_id,
    occu,
    oday,
    odo,
    oper,
    owner_operator_id,
    received_at,
    route,
    route_id,
    seq,
    sid,
    signal_group_id,
    spd,
    start,
    stop,
    tlp_att_seq,
    tlp_decision,
    tlp_frequency,
    tlp_line_config_id,
    tlp_point_config_id,
    tlp_priority_level,
    tlp_protocol,
    tlp_reason,
    tlp_request_id,
    tlp_request_type,
    tlp_signal_group_nbr,
    topic_latitude,
    topic_longitude,
    topic_prefix,
    topic_version,
    tsi,
    point_timestamp,
    unique_vehicle_id,
    uuid,
    veh,
    vehicle_number
  FROM staging.tlr_raw
  ORDER BY route_id, vehicle_number
  ON CONFLICT DO NOTHING;
$procedure$;

COMMENT ON PROCEDURE staging.import_and_normalize_tlr IS 'Procedure to copy data from staging schema to tlr schema.';
