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
$procedure$;

COMMENT ON PROCEDURE staging.import_and_normalize_hfp IS 'Procedure to copy data from staging schema to hfp schema.';
