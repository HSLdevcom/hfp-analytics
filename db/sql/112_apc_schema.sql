CREATE SCHEMA apc;

COMMENT ON SCHEMA apc is 'Contains APC data (automatic passenger counting)';


CREATE TABLE apc.apc (
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
  vehicle_load          smallint,
  vehicle_load_ratio    real,
  doors_data            jsonb,
  count_quality         text,
  geom                  geometry(POINT, 3067),
  CONSTRAINT apc_pkey PRIMARY KEY (point_timestamp, vehicle_operator_id, vehicle_number)
);


COMMENT ON COLUMN apc.apc.point_timestamp IS 'Absolute timestamp of the observation.';
COMMENT ON COLUMN apc.apc.received_at IS 'Absolute timestamp when the underlying observation was received by server.';
COMMENT ON COLUMN apc.apc.vehicle_operator_id IS 'Id of the operator who owns the vehicle. `operator_id` in APC topic.';
COMMENT ON COLUMN apc.apc.vehicle_number IS 'Vehicle number, unique within operator. `vehicle_number` in APC payload.';
COMMENT ON COLUMN apc.apc.transport_mode IS 'Mode of the vehicle. `transport_mode` in APC topic.';
COMMENT ON COLUMN apc.apc.route_id IS 'Route identifier originating from Jore. `route_id` in APC payload.';
COMMENT ON COLUMN apc.apc.direction_id IS 'Direction identifier originating from Jore: 1 or 2. `direction_id` in APC payload.';
COMMENT ON COLUMN apc.apc.oday IS 'Operating date originating from Jore. `oday` in APC payload.';
COMMENT ON COLUMN apc.apc."start" IS 'Start time on the operating date, HH:MM:SS. `start` in APC payload.
N.B. APC uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN apc.apc.observed_operator_id IS 'Id of the operator the journey was assigned to. `oper` in APC payload.';
COMMENT ON COLUMN apc.apc.stop IS 'Id of the stop that the APC point was related to.';
COMMENT ON COLUMN apc.apc.vehicle_load IS 'Passenger count of the vehicle.';
COMMENT ON COLUMN apc.apc.vehicle_load_ratio IS 'The ratio how full the vehicle is.';
COMMENT ON COLUMN apc.apc.doors_data IS 'JSON field to contain detailed information how many,
what kind of, and through which doors, the passengers entered and hopped off the vehicle.';
COMMENT ON COLUMN apc.apc.count_quality IS 'Id of the stop that the APC point was related to.';
COMMENT ON COLUMN apc.apc.geom IS 'Vehicle position point in ETRS-TM35 coordinates.';

SELECT create_hypertable('apc.apc', 'point_timestamp', chunk_time_interval => INTERVAL '24 hours');


CREATE INDEX apc_timestamp_idx ON apc.apc (point_timestamp DESC); -- This could be covered by other indices?
COMMENT ON INDEX apc.apc_timestamp_idx IS 'Index timestamp filtering.';
CREATE INDEX apc_route_vehicle_idx ON apc.apc (route_id, vehicle_operator_id, vehicle_number, point_timestamp DESC);
COMMENT ON INDEX apc.apc_route_vehicle_idx IS 'Index for apc raw data queries.';
