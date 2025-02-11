CREATE SCHEMA tlp;

COMMENT ON SCHEMA tlp is 'Contains tlp data';

CREATE TABLE tlp.tlp (
  event_type            text,
  location_quality_method text,
  latitude              double precision,
  longitude             double precision,
  oday                  date,
  oper                  integer,
  direction_id          smallint,
  received_at           timestamptz,
  route_id              text,
  sid                   integer,
  signal_group_id       integer,
  start                 text,
  tlp_att_seq           integer,
  tlp_decision          text,
  tlp_priority_level    text,
  tlp_reason            text,
  tlp_request_type      text,
  tlp_signal_group_nbr  integer,
  point_timestamp       timestamptz,
  vehicle_number        integer NOT NULL
);


COMMENT ON COLUMN tlp.tlp.point_timestamp IS 'Absolute timestamp of the observation.';
COMMENT ON COLUMN tlp.tlp.received_at IS 'Absolute timestamp when the underlying observation was received by server.';
COMMENT ON COLUMN tlp.tlp.vehicle_number IS 'Vehicle number, unique within operator. `vehicle_number` in tlp payload.';
COMMENT ON COLUMN tlp.tlp.route_id IS 'Route identifier originating from Jore. `route_id` in tlp payload.';
COMMENT ON COLUMN tlp.tlp.direction_id IS 'Direction identifier originating from Jore: 1 or 2. `direction_id` in tlp payload.';
COMMENT ON COLUMN tlp.tlp.oday IS 'Operating date originating from Jore. `oday` in tlp payload.';
COMMENT ON COLUMN tlp.tlp."start" IS 'Start time on the operating date, HH:MM:SS. `start` in tlp payload.
N.B. tlp uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';

SELECT create_hypertable('tlp.tlp', 'point_timestamp', chunk_time_interval => INTERVAL '24 hours');


CREATE INDEX tlp_route_vehicle_idx ON tlp.tlp (route_id, vehicle_number, point_timestamp DESC);
COMMENT ON INDEX tlp.tlp_route_vehicle_idx IS 'Index for tlp raw data queries.';
