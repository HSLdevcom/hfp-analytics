CREATE SCHEMA staging;
COMMENT ON SCHEMA staging IS 'Schema containing temporal data to be imported.';


CREATE TABLE staging.hfp_raw (
	tst                   timestamptz   NOT NULL,
	event_type            text          NOT NULL,
	received_at           timestamptz,
	vehicle_operator_id   smallint      NOT NULL,
	vehicle_number        smallint      NOT NULL,
	transport_mode        text,
	route_id              text,
	direction_id          smallint,
	oday                  date,
	"start"               interval,
	observed_operator_id  smallint,
	odo                   real,
	drst                  bool,
	loc                   text,
	stop                  integer,
	longitude             double precision,
  latitude              double precision
);
COMMENT ON TABLE staging.hfp_raw IS 'Table where the client copies hfp data to be imported to hfp schema.';


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
    drst,
    loc,
    stop,
    ST_Transform( ST_SetSRID( ST_MakePoint(longitude, latitude), 4326), 3067)
  FROM staging.hfp_raw
  ON CONFLICT DO NOTHING;

  INSERT INTO hfp.assumed_monitored_vehicle_journey (
    vehicle_operator_id, vehicle_number, transport_mode, route_id, direction_id, oday, "start", observed_operator_id, min_timestamp, max_timestamp
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
    max(tst) AS max_timestamp
  -- (Add further aggregates such as N of hfp_point rows here, if required later.
  -- Be careful about min_tst, because aggregate might not give all records, if there were ones before min_tst.
  FROM staging.hfp_raw
  WHERE
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
    modified_at = now()
  WHERE
  -- Update only if values are actually changed, so that modified_at -field shows the correct time.
    assumed_monitored_vehicle_journey.min_timestamp != EXCLUDED.min_timestamp OR
  	assumed_monitored_vehicle_journey.max_timestamp != EXCLUDED.max_timestamp;
$procedure$;

COMMENT ON PROCEDURE staging.import_and_normalize_hfp IS 'Procedure to copy data from staging schema to hfp schema.';
