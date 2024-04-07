CREATE SCHEMA tlp;

COMMENT ON SCHEMA tlp is 'Contains tlp data';

CREATE TABLE tlp.tlp (
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


COMMENT ON COLUMN tlp.tlp.point_timestamp IS 'Absolute timestamp of the observation.';
COMMENT ON COLUMN tlp.tlp.received_at IS 'Absolute timestamp when the underlying observation was received by server.';
COMMENT ON COLUMN tlp.tlp.vehicle_number IS 'Vehicle number, unique within operator. `vehicle_number` in tlp payload.';
COMMENT ON COLUMN tlp.tlp.mode IS 'Mode of the vehicle. `mode` in tlp topic.';
COMMENT ON COLUMN tlp.tlp.route_id IS 'Route identifier originating from Jore. `route_id` in tlp payload.';
COMMENT ON COLUMN tlp.tlp.direction_id IS 'Direction identifier originating from Jore: 1 or 2. `direction_id` in tlp payload.';
COMMENT ON COLUMN tlp.tlp.oday IS 'Operating date originating from Jore. `oday` in tlp payload.';
COMMENT ON COLUMN tlp.tlp."start" IS 'Start time on the operating date, HH:MM:SS. `start` in tlp payload.
N.B. tlp uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN tlp.tlp.stop IS 'Id of the stop that the tlp point was related to.';

SELECT create_hypertable('tlp.tlp', 'point_timestamp', chunk_time_interval => INTERVAL '24 hours');


CREATE INDEX tlp_route_vehicle_idx ON tlp.tlp (route_id, vehicle_operator_id, vehicle_number, point_timestamp DESC);
COMMENT ON INDEX tlp.tlp_route_vehicle_idx IS 'Index for tlp raw data queries.';
