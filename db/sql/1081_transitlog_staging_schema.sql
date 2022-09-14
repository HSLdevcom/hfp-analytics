CREATE SCHEMA transitlog_stg;
COMMENT ON SCHEMA transitlog_stg IS
'Staging tables and functions for importing Jore data
from Transitlog db to planned schema.';

CREATE TABLE transitlog_stg.route (
  route_id text NOT NULL,
  direction text NOT NULL,
  date_begin date NOT NULL,
  date_end date NOT NULL,
  type text,
  date_modified date,
  date_imported timestamptz,
  -- Populated here:
  route_uuid uuid,
  valid_during daterange,
  transitlog_read_at timestamptz DEFAULT now(),

  -- Pkey corresponding to Transitlog.
  PRIMARY KEY (route_id, direction, date_begin, date_end)
);
-- -> Destination: planned.route

CREATE TABLE transitlog_stg.route_segment (
  route_id text NOT NULL,
  direction text NOT NULL,
  date_begin date NOT NULL,
  date_end date NOT NULL,
  stop_id text NOT NULL,
  stop_index integer NOT NULL,
  date_modified date,
  date_imported timestamptz,
  -- Populated here:
  stop_in_pattern_uuid uuid,
  route_uuid uuid,
  transitlog_read_at timestamptz DEFAULT now(),

  -- Pkey corresponding to Transitlog.
  PRIMARY KEY (route_id, direction, date_begin, date_end, stop_index)
);
-- -> Destination: planned.stop_point_in_journey_pattern

CREATE TABLE transitlog_stg.exception_days_calendar (
  date_in_effect date PRIMARY KEY,
  exception_day_type text,
  day_type text,
  exclusive integer,
  date_imported timestamptz,
  -- Populated here:
  transitlog_read_at timestamptz DEFAULT now(),
);
-- -> Destination: planned.service_calendar

CREATE TABLE transitlog_stg.replacement_days_calendar (
  date_in_effect date NOT NULL,
  scope text NOT NULL,
  replacing_day_type text,
  day_type text,
  time_begin text,
  time_end text,
  date_imported,
  -- Populated here:
  transitlog_read_at timestamptz DEFAULT now(),

  PRIMARY KEY (date_in_effect, scope)
);
-- -> Destination: planned.service_calendar
