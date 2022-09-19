CREATE SCHEMA planned;
COMMENT ON SCHEMA planned IS
'Models dimensions of planned transit operations: 
calendar, stops, routes, journey patterns, and service journeys.';

CREATE TABLE planned.service_calendar (
  calendar_uuid         uuid PRIMARY KEY,
  day_type              text NOT NULL,
  operating_day         date NOT NULL,

  UNIQUE (day_type, operating_day)
);
COMMENT ON TABLE planned.service_calendar IS
'DAY TYPES assigned to OPERATING DAYS.
This table allows realizing SERVICE JOURNEYS of a day type
as DATED VEHICLE JOURNEYS of operating days (that can last over 24 hrs).';

-- TODO: Create the table below
--       once we start to really need _stop point related attributes_.
--       Until then, stop_id values in other tables w/o fkey references will do.
-- CREATE TABLE planned.scheduled_stop_point ()

CREATE TABLE planned.route (
  route_uuid            uuid PRIMARY KEY,
  route_id              text NOT NULL,
  direction             smallint NOT NULL,
  valid_during          daterange NOT NULL,
  import_history        jsonb,
  modified_at           timestamptz DEFAULT now(),

  EXCLUDE USING GIST (route_id WITH =, direction WITH =, valid_during WITH &&)
);
COMMENT ON TABLE planned.route IS
'A service path of stops and links through the transit network.
Only one combination (route_id, direction) may be in effect during an operating date (valid_during).';

CREATE TABLE planned.stop_role (
  stop_role_key         smallint PRIMARY KEY CHECK (stop_role_key BETWEEN 1 AND 4),
  stop_role             text NOT NULL
);
COMMENT ON TABLE planned.stop_role IS
'Enum values for stop roles in journey patterns (first/last, timing point).';
INSERT INTO planned.stop_role VALUES
  (1, 'FIRST'),
  (2, 'TIMING_POINT'),
  (3, 'LAST'),
  (4, 'NORMAL');

CREATE TABLE planned.stop_point_in_journey_pattern (
  stop_in_pattern_uuid  uuid PRIMARY KEY,
  route_uuid            uuid NOT NULL REFERENCES planned.route(route_uuid),
  stop_sequence         smallint NOT NULL CHECK (stop_sequence > 0),
  stop_point_id         text NOT NULL,
  stop_role_key         smallint NOT NULL REFERENCES planned.stop_role(stop_role_key),

  UNIQUE (route_uuid, stop_sequence)
);
COMMENT ON TABLE planned.stop_point_in_journey_pattern IS
'Defines an ordered list of stops that a route shall visit.';

CREATE TABLE planned.service_journey (
  service_journey_uuid  uuid PRIMARY KEY,
  route_uuid            uuid NOT NULL REFERENCES planned.route(route_uuid),
  calendar_uuid         uuid NOT NULL REFERENCES planned.service_calendar(calendar_uuid),
  journey_start_30h     interval NOT NULL,

  UNIQUE (route_uuid, calendar_uuid, journey_start_30h)
);
COMMENT ON TABLE planned.service_journey IS
'A passenger carrying vehicle journey of a DAY TYPE following a ROUTE.';

CREATE TABLE planned.timetabled_passing_time (
  service_journey_uuid  uuid NOT NULL REFERENCES planned.service_journey(service_journey_uuid),
  stop_in_pattern_uuid  uuid NOT NULL REFERENCES planned.stop_point_in_journey_pattern(stop_in_pattern_uuid),
  arrival_30h           interval,
  departure_30h         interval,

  PRIMARY KEY (service_journey_uuid, stop_in_pattern_uuid)
);
COMMENT ON TABLE planned.timetabled_passing_time IS
'Planned stop call times of SERVICE JOURNEYS via their
STOP POINTS IN JOURNEY PATTERN.';

-- TODO: Extra journey model.