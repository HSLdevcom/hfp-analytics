CREATE SCHEMA hfp;

COMMENT ON SCHEMA hfp IS 'Models transit vehicle state, position and event data (High-Frequency Positioning).';


CREATE TABLE hfp.hfp_point (
  point_timestamp       timestamptz   NOT NULL,
  hfp_event             text          NOT NULL,
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
  hdg                   integer,
  geom                  geometry(POINT, 3067),
  CONSTRAINT hfp_point_pkey PRIMARY KEY (point_timestamp, vehicle_operator_id, vehicle_number, hfp_event)
);
COMMENT ON TABLE hfp.hfp_point IS 'State of a transit vehicle at a time instant, based on HFP.';

COMMENT ON COLUMN hfp.hfp_point.point_timestamp IS 'Absolute timestamp of the observation.';
COMMENT ON COLUMN hfp.hfp_point.hfp_event IS 'HFP event triggered by the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.received_at IS 'Absolute timestamp when the underlying observation was received by server.';
COMMENT ON COLUMN hfp.hfp_point.vehicle_operator_id IS 'Id of the operator who owns the vehicle. `operator_id` in HFP topic.';
COMMENT ON COLUMN hfp.hfp_point.vehicle_number IS 'Vehicle number, unique within operator. `vehicle_number` in HFP topic.';
COMMENT ON COLUMN hfp.hfp_point.transport_mode IS 'Mode of the vehicle. `transport_mode` in HFP topic.';
COMMENT ON COLUMN hfp.hfp_point.route_id IS 'Route identifier originating from Jore. `route_id` in HFP topic.';
COMMENT ON COLUMN hfp.hfp_point.direction_id IS 'Direction identifier originating from Jore: 1 or 2. `direction_id` in HFP topic.';
COMMENT ON COLUMN hfp.hfp_point.oday IS 'Operating date originating from Jore. `oday` in HFP payload.';
COMMENT ON COLUMN hfp.hfp_point."start" IS 'Start time on the operating date, HH:MM:SS. `start` in HFP payload.
N.B. HFP uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN hfp.hfp_point.observed_operator_id IS 'Id of the operator the journey was assigned to. `oper` in HFP payload.';
COMMENT ON COLUMN hfp.hfp_point.odo IS 'Odometer value of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.spd IS 'Speed of the vehicle (m/s).';
COMMENT ON COLUMN hfp.hfp_point.drst IS 'Door status of the vehicle. TRUE if any door is open, FALSE if all closed, NULL if unknown.';
COMMENT ON COLUMN hfp.hfp_point.loc IS 'Source of the vehicle position information. Ideally GPS.';
COMMENT ON COLUMN hfp.hfp_point.stop IS 'Id of the stop that the HFP point was related to.';
COMMENT ON COLUMN hfp.hfp_point.geom IS 'Vehicle position point in ETRS-TM35 coordinates.';


SELECT create_hypertable('hfp.hfp_point', 'point_timestamp', chunk_time_interval => INTERVAL '6 hours');

CREATE INDEX hfp_point_route_vehicle_idx ON hfp.hfp_point (route_id, vehicle_operator_id, vehicle_number, point_timestamp DESC);
COMMENT ON INDEX hfp.hfp_point_route_vehicle_idx IS 'Index for hfp raw data queries.';
CREATE INDEX hfp_point_event_idx ON hfp.hfp_point (hfp_event, point_timestamp DESC);
COMMENT ON INDEX hfp.hfp_point_event_idx IS 'Index for hfp event filter (used at least by the stop analysis).';

-- The similar schema as previously, but this time for invalid points
CREATE TABLE hfp.hfp_point_invalid (LIKE hfp.hfp_point INCLUDING ALL);
-- Timescaledb hypertable for hfp data. Invalid data shouldn't need this because there's (hopefully not so much data in it.)


CREATE TABLE hfp.assumed_monitored_vehicle_journey (
  vehicle_operator_id   smallint      NOT NULL,
  vehicle_number        integer       NOT NULL,
  transport_mode        text          NOT NULL,
  route_id              text          NOT NULL,
  direction_id          smallint      NOT NULL,
  oday                  date          NOT NULL,
  "start"               interval      NOT NULL,
  observed_operator_id  smallint      NOT NULL,
  min_timestamp         timestamptz   NOT NULL,
  max_timestamp         timestamptz   NOT NULL,
  arr_count             integer       NULL        DEFAULT 0,
  modified_at           timestamptz   NULL        DEFAULT now(),
  CONSTRAINT assumed_monitored_vehicle_journey_pkey PRIMARY KEY (vehicle_operator_id, vehicle_number, oday, route_id, direction_id, "start", observed_operator_id)
);

COMMENT ON TABLE hfp.assumed_monitored_vehicle_journey IS
'Assumed monitored vehicle journey (or part of a journey) with the same vehicle
including min and max timestamps. Assumed here means that this journey might
be invalid (e.g. driver accidentally logged into a wrong departure)';

CREATE INDEX assumed_monitored_vehicle_journey_oday_idx ON hfp.assumed_monitored_vehicle_journey USING btree(oday);

CREATE TABLE hfp.vehicle_analysis (
  vehicle_number INTEGER NOT NULL,
  vehicle_operator_id INTEGER NOT NULL,
  date date NOT NULL,
  drst_null_ratio FLOAT,
  drst_true_ratio FLOAT,
  drst_false_ratio FLOAT,
  door_error_types TEXT[],
  door_error_events JSONB,
  odo_exists_ratio FLOAT,
  odo_null_ratio FLOAT,
  odo_error_types TEXT[],
  odo_error_events JSONB,
  loc_null_ratio FLOAT,
  loc_gps_ratio FLOAT,
  loc_dr_ratio FLOAT,
  loc_error_types TEXT[],
  loc_error_events JSONB,
  events_amount INTEGER
);

CREATE INDEX vehicle_analysis_date_vehicle_number_op_id_idx
  ON hfp.vehicle_analysis (date, vehicle_number, vehicle_operator_id);

ALTER TABLE hfp.vehicle_analysis
  ADD CONSTRAINT date_vehicle_number_op_id_unique
  UNIQUE (date, vehicle_number, vehicle_operator_id);