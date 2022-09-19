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


-- NOTE: Split this monolith function into more manageable pieces
--       if necessary, this is the first iteration.
CREATE FUNCTION transitlog_stg.prepare_all()
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $function$
BEGIN
  -- ### route ###
  -- Generate valid_during values.
  UPDATE transitlog_stg.route
  SET valid_during = daterange(date_begin, date_end);

  -- Delete conflicting routes.
  CREATE VIEW transitlog_stg.route_with_version_conflict_indicator AS (
    SELECT
      *,
      coalesce(
        valid_during && lead(valid_during) OVER (PARTITION BY route_id, direction ORDER BY valid_during, date_modified, date_imported),
        false
        ) AS does_conflict
    FROM transitlog_stg.route
    ORDER BY route_id, direction, valid_during, date_modified, date_imported
  );
  COMMENT ON VIEW transitlog_stg.route_with_version_conflict_indicator IS
  'Shows whether a route conflicts with its next version in time, when ordered by
  validity 1) date period, 2) Jore modification date, and 3) Transitlog import timestamp.';

  DELETE FROM transitlog_stg.route
  WHERE (route_id, direction, date_begin, date_end) IN (
    SELECT route_id, direction, date_begin, date_end
    FROM transitlog_stg.route_with_version_conflict_indicator
    WHERE does_conflict
  );

  -- Generate route_uuid values.
  UPDATE transitlog_stg.route
  SET route_uuid = md5(concat_ws('_', route_id, direction, date_begin, date_end))::uuid;

  -- ### route_segment ###
  -- Delete segments of deleted routes.
  DELETE FROM transitlog_stg.route_segment
  WHERE (route_id, direction, date_begin, date_end) NOT IN (
    SELECT route_id, direction, date_begin, date_end
    FROM transitlog_stg.route
  );

  -- Generate route_uuid values.
  UPDATE transitlog_stg.route_segment
  SET route_uuid = md5(concat_ws('_', route_id, direction, date_begin, date_end))::uuid;

  -- Generate stop_in_pattern-uuid values.
  UPDATE transitlog_stg.route_segment
  SET stop_in_pattern_uuid = md5(concat_ws('_', route_uuid, stop_index))::uuid;

  -- Set stop_role_key values.
  UPDATE transitlog_stg.route_segment AS rs
  SET stop_role_key = CASE
      WHEN rs.stop_index = 1 THEN 1
      WHEN lsi.is_last THEN 2
      -- Both timing_stop_type values 1 and 2 seem to correspond to stops used
      -- as regulated timing points on the route; no documentation found
      -- for the difference of 1 and 2, apparently they inherit from Jore.  
      WHEN rs.timing_stop_type IN (1, 2) THEN 3
      ELSE 4
    END
  FROM (
    SELECT
      route_id, direction, date_begin, date_end, stop_index,
      (stop_index = (max(stop_index) OVER (PARTITION BY (route_id, direction, date_begin, date_end)))) AS is_last
    FROM transitlog_stg.route_segment
  ) AS lsi
  WHERE (rs.route_id, rs.direction, rs.date_begin, rs.date_end, rs.stop_index)
    = (lsi.route_id, lsi.direction, lsi.date_begin, lsi.date_end, lsi.stop_index);

  -- ### exception_days_calendar, replacement_days_calendar ###
  -- TODO: Create calendar_id + dates-in-effect entries by day types
  --       and exceptions.

  -- ### departure ###
  -- Delete departures of deleted routes.
  DELETE FROM transitlog_stg.departure
  WHERE (route_id, direction, date_begin, date_end) NOT IN (
    SELECT route_id, direction, date_begin, date_end
    FROM transitlog_stg.route
  );

  -- Generate route_uuid values.
  UPDATE transitlog_stg.departure
  SET route_uuid = md5(concat_ws('_', route_id, direction, date_begin, date_end))::uuid;

  -- Generate 30h values.
  UPDATE transitlog_stg.departure
  SET
    arrival_30h = format(
      '%s %s:%s:00',
      CASE WHEN arrival_is_next_day THEN 1 ELSE 0 END,
      arrival_hours,
      arrival_minutes
      )::interval,
    departure_30h = format(
      '%s %s:%s:00',
      CASE WHEN is_next_day THEN 1 ELSE 0 END,
      hours,
      minutes
      )::interval;

  -- TODO:
  -- * Generate service_journey_uuid (after calendar stuff is done)
  -- * Create interim table for unique service_journey entries, and there:
  --   * calendar_uuid
  --   * journey_start_30h
END;
$function$;
COMMENT ON FUNCTION transitlog_stg.prepare_all IS
'Prepares the staging data so it is ready for insertion into "planned" schema.';