CREATE SCHEMA tlr;

COMMENT ON SCHEMA tlr is 'Contains tlr data';

CREATE TABLE tlr.tlr (
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


COMMENT ON COLUMN tlr.tlr.point_timestamp IS 'Absolute timestamp of the observation.';
COMMENT ON COLUMN tlr.tlr.received_at IS 'Absolute timestamp when the underlying observation was received by server.';
COMMENT ON COLUMN tlr.tlr.vehicle_number IS 'Vehicle number, unique within operator. `vehicle_number` in tlr payload.';
COMMENT ON COLUMN tlr.tlr.mode IS 'Mode of the vehicle. `mode` in tlr topic.';
COMMENT ON COLUMN tlr.tlr.route_id IS 'Route identifier originating from Jore. `route_id` in tlr payload.';
COMMENT ON COLUMN tlr.tlr.direction_id IS 'Direction identifier originating from Jore: 1 or 2. `direction_id` in tlr payload.';
COMMENT ON COLUMN tlr.tlr.oday IS 'Operating date originating from Jore. `oday` in tlr payload.';
COMMENT ON COLUMN tlr.tlr."start" IS 'Start time on the operating date, HH:MM:SS. `start` in tlr payload.
N.B. tlr uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN tlr.tlr.stop IS 'Id of the stop that the tlr point was related to.';

SELECT create_hypertable('tlr.tlr', 'point_timestamp', chunk_time_interval => INTERVAL '24 hours');


CREATE INDEX tlr_route_vehicle_idx ON tlr.tlr (route_id, vehicle_operator_id, vehicle_number, point_timestamp DESC);
COMMENT ON INDEX tlr.tlr_route_vehicle_idx IS 'Index for tlr raw data queries.';
