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
  read_from_transitlog_at timestamptz DEFAULT now(),

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
  timing_stop_type integer NOT NULL,
  date_modified date,
  date_imported timestamptz,
  -- Populated here:
  stop_in_pattern_uuid uuid,
  route_uuid uuid,
  stop_role_key smallint,
  read_from_transitlog_at timestamptz DEFAULT now(),

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
  read_from_transitlog_at timestamptz DEFAULT now()
);
-- -> Destination: planned.service_calendar

CREATE TABLE transitlog_stg.replacement_days_calendar (
  date_in_effect date NOT NULL,
  scope text NOT NULL,
  replacing_day_type text,
  day_type text,
  time_begin text,
  time_end text,
  date_imported timestamptz,
  -- Populated here:
  read_from_transitlog_at timestamptz DEFAULT now(),

  PRIMARY KEY (date_in_effect, scope)
);
-- -> Destination: planned.service_calendar

CREATE TABLE transitlog_stg.departure (
  route_id text NOT NULL,
  direction text NOT NULL,
  date_begin date NOT NULL,
  date_end date NOT NULL,
  hours integer NOT NULL,
  minutes integer NOT NULL,
  stop_id text NOT NULL,
  day_type text NOT NULL,
  extra_departure text NOT NULL,
  is_next_day boolean NOT NULL,
  arrival_is_next_day boolean NOT NULL,
  arrival_hours integer,
  arrival_minutes integer,
  operator_id text,
  date_imported timestamptz,
  -- Populated here:
  service_journey_uuid uuid,
  route_uuid uuid,
  stop_in_pattern_uuid uuid,
  arrival_30h interval,
  departure_30h interval,
  read_from_transitlog_at timestamptz DEFAULT now(),

  PRIMARY KEY (route_id, direction, date_begin, date_end, hours, minutes, stop_id, day_type, extra_departure)
);
-- -> Destination: planned.service_journey
--                 planned.timetabled_passing_time


-- NOTE:
-- This is an interim function for WIP before the integration via Python
-- is in place. It requires project's ./data/transitlog/ to be mapped to db service's /import/transitlog/,
-- and Transitlog sample csv files available there.
CREATE FUNCTION transitlog_stg.read_all()
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $function$
BEGIN
  COPY transitlog_stg.route (route_id, direction, date_begin, date_end, type, date_modified, date_imported) 
  FROM '/import/transitlog/route.csv' 
  WITH CSV HEADER;

  COPY transitlog_stg.route_segment (route_id, direction, date_begin, date_end, stop_id, stop_index, timing_stop_type, date_modified, date_imported) 
  FROM '/import/transitlog/route_segment.csv' 
  WITH CSV HEADER;

  COPY transitlog_stg.exception_days_calendar (date_in_effect, exception_day_type, day_type, exclusive, date_imported)
  FROM '/import/transitlog/exception_days_calendar.csv'
  WITH CSV HEADER;

  COPY transitlog_stg.replacement_days_calendar (date_in_effect, scope, replacing_day_type, day_type, time_begin, time_end, date_imported)
  FROM '/import/transitlog/replacement_days_calendar.csv'
  WITH CSV HEADER;

  COPY transitlog_stg.departure (route_id, direction, date_begin, date_end, hours, minutes, stop_id, day_type, extra_departure, 
    is_next_day, arrival_is_next_day, arrival_hours, arrival_minutes, operator_id, date_imported)
  FROM '/import/transitlog/departure.csv'
  WITH CSV HEADER;
END;
$function$;


CREATE FUNCTION transitlog_stg.truncate_all()
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $function$
BEGIN
  TRUNCATE transitlog_stg.departure;
  TRUNCATE transitlog_stg.exception_days_calendar;
  TRUNCATE transitlog_stg.replacement_days_calendar;
  TRUNCATE transitlog_stg.route;
  TRUNCATE transitlog_stg.route_segment;
END;
$function$;
COMMENT ON FUNCTION transitlog_stg.truncate_all IS
'Cleans up all staging tables in this schema. Should be run before every new data import.';