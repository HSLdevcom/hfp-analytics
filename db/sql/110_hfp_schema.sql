CREATE SCHEMA hfp;

COMMENT ON SCHEMA hfp IS 'Models transit vehicle state, position and event data (High-Frequency Positioning).';


CREATE TABLE hfp.hfp_point (
	point_timestamp       timestamptz   NOT NULL,
	hfp_event             text          NOT NULL,
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
	geom                  geometry(POINT, 3067),

  CONSTRAINT hfp_point_pkey PRIMARY KEY (point_timestamp, vehicle_operator_id, vehicle_number, hfp_event)
);
COMMENT ON TABLE hfp.hfp_point IS 'State of a transit vehicle at a time instant, based on HFP.';

COMMENT ON COLUMN hfp.hfp_point.point_timestamp IS 'Absolute timestamp of the observation.';
COMMENT ON COLUMN hfp.hfp_point.hfp_event IS 'HFP event triggered by the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.received_at IS 'Absolute timestamp when the underlying observation was received by server.';
COMMENT ON COLUMN hfp.vehicle.vehicle_operator_id IS 'Id of the operator who owns the vehicle. `operator_id` in HFP topic.';
COMMENT ON COLUMN hfp.vehicle.vehicle_number IS 'Vehicle number, unique within operator. `vehicle_number` in HFP topic.';
COMMENT ON COLUMN hfp.vehicle.transport_mode IS 'Mode of the vehicle. `transport_mode` in HFP topic.';
COMMENT ON COLUMN hfp.observed_journey.route_id IS 'Route identifier originating from Jore. `route_id` in HFP topic.';
COMMENT ON COLUMN hfp.observed_journey.direction_id IS 'Direction identifier originating from Jore: 1 or 2. `direction_id` in HFP topic.';
COMMENT ON COLUMN hfp.observed_journey.oday IS 'Operating date originating from Jore. `oday` in HFP payload.';
COMMENT ON COLUMN hfp.observed_journey."start" IS 'Start time on the operating date, HH:MM:SS. `start` in HFP payload.
N.B. HFP uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN hfp.observed_journey.observed_operator_id IS 'Id of the operator the journey was assigned to. `oper` in HFP payload.';
COMMENT ON COLUMN hfp.hfp_point.odo IS 'Odometer value of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.drst IS 'Door status of the vehicle. TRUE if any door is open, FALSE if all closed, NULL if unknown.';
COMMENT ON COLUMN hfp.hfp_point.loc IS 'Source of the vehicle position information. Ideally GPS.';
COMMENT ON COLUMN hfp.hfp_point.stop IS 'Id of the stop that the HFP point was related to.';
COMMENT ON COLUMN hfp.hfp_point.geom IS 'Vehicle position point in ETRS-TM35 coordinates.';


SELECT create_hypertable('hfp.hfp_point', 'point_timestamp', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX hfp.hfp_point_journey_idx ON hfp.hfp_point (oday, route_id, direction_id, "start", point_timestamp DESC);
COMMENT ON INDEX hfp.hfp_point_journey_idx IS 'Index for journey related columns.'



CREATE TABLE hfp.assumed_monitored_vehicle_journey (
	vehicle_operator_id   smallint      NOT NULL,
	vehicle_number        smallint      NOT NULL,
	transport_mode        text,
	route_id              text,
	direction_id          smallint,
	oday                  date,
	"start"               interval,
	observed_operator_id  smallint,
	min_timestamp timestamptz NOT NULL,
	max_timestamp timestamptz NOT NULL,
	modified_at timestamptz NULL DEFAULT now(),

	CONSTRAINT assumed_monitored_vehicle_journey_pkey PRIMARY KEY (vehicle_operator_id, vehicle_number, oday, route_id, direction_id, "start")
);

COMMENT ON TABLE hfp.assumed_monitored_vehicle_journey IS
'Assumed monitored vehicle journey (or part of a journey) with the same vehicle
including min and max timestamps. Assumed here means that this journey might
be invalid (e.g. driver accidentally logged into a wrong departure)';


CREATE OR REPLACE VIEW hfp.view_as_original_hfp_event AS (
  SELECT
    point_timestamp AS tst,
    hfp_event AS event_type,
    received_at,
    vehicle_operator_id,
    vehicle_number,
    transport_mode,
    route_id,
    direction_id,
    oday,
    start,
    observed_operator_id,
    odo,
    drst,
    loc,
    stop,
    ST_X(ST_Transform(geom, 4326)) AS longitude,
    ST_Y(ST_Transform(geom, 4326)) AS latitude
  FROM hfp.hfp_point
);

COMMENT ON VIEW hfp.view_as_original_hfp_event IS 'Exposes HFP points named like in original HFP data format.';


CREATE SCHEMA staging;
COMMENT ON SCHEMA staging IS 'Schema containing temporal data to be imported.'


CREATE TABLE staging.hfp_raw (
	tst                   timestamptz   NOT NULL,
	hfp_event             text          NOT NULL,
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
	longitude             real,
  latitude              real
);
COMMENT ON TABLE staging.hfp_raw IS 'Table where the client copies hfp data to be imported to hfp schema.'


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
