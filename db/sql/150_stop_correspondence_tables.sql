-- 
-- Tables for stop correspondence analysis.
-- TODO: Move these tables later to a separate schema,
--       e.g. CREATE SCHEMA stopcorr; CREATE TABLE stopcorr.observation ... etc.
--       This way, objects related to particular business cases
--       are easy to group and identify together on db side.
-- 

--
-- HFP observations
--
CREATE TABLE observation (
  tst timestamptz NOT NULL,
  event text NOT NULL,
  oper integer NOT NULL,
  veh integer NOT NULL,
  route text,
  dir smallint,
  oday date,
  start time,
  stop_id integer,
  stop_id_guessed boolean,
  dist_to_jore_point_m real,
  dist_to_median_point_m real,
  long double precision,
  lat double precision,
  geom geometry(POINT, 3067) GENERATED ALWAYS AS (ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3067)) STORED,

  PRIMARY KEY (tst, event, oper, veh)
);

CREATE INDEX ON observation USING BTREE (stop_id, stop_id_guessed)
  WHERE stop_id IS NOT NULL;
CREATE INDEX ON observation USING GIST (geom);

COMMENT ON TABLE observation IS
'DOO and DOC events imported from HFP. See Digitransit doc for the first columns.';
COMMENT ON COLUMN observation.stop_id IS
'Long stop id from Jore/Digitransit, same as HFP "stop".';
COMMENT ON COLUMN observation.stop_id_guessed IS
'If false, stop_id comes from HFP/LIJ. If true, it has been "guessed" afterwards by the analysis process.';
COMMENT ON COLUMN observation.dist_to_jore_point_m IS
'Distance (m) to the corresponding jore_stop(stop_id).';
COMMENT ON COLUMN observation.dist_to_median_point_m IS
'Distance (m) to the corresponding stop_median(stop_id).';
COMMENT ON COLUMN observation.long IS
'WGS84 longitude (from import file).';
COMMENT ON COLUMN observation.lat IS
'WGS84 latitude (from import file).';
COMMENT ON COLUMN observation.geom IS
'Observation POINT geometry in ETRS-TM35 coordinate system, generated from long & lat.';

--
-- Jore / Digitransit stops
--
-- Note that only one temporal snapshot of stops can be stored at a time,
-- i.e. no multiple versions of one stop_id.
--

CREATE TABLE jore_stop (
  stop_id integer PRIMARY KEY,
  stop_code text,
  stop_name text,
  parent_station integer,
  stop_mode text,
  route_dirs_via_stop text[],
  date_imported date DEFAULT CURRENT_DATE,
  long double precision,
  lat double precision,
  geom geometry(POINT, 3067) GENERATED ALWAYS AS (ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3067)) STORED
);

CREATE INDEX ON jore_stop USING GIST (geom);

COMMENT ON TABLE jore_stop IS
'Current HSL stops from Jore/Digitransit. See Digitransit for the first columns.';
COMMENT ON COLUMN jore_stop.route_dirs_via_stop IS
'Route-direction combinations using the stop, according to Digitransit.';
COMMENT ON COLUMN jore_stop.date_imported IS
'Date on which the stop was imported from Digitransit.';
COMMENT ON COLUMN observation.long IS
'WGS84 longitude (from Digitransit).';
COMMENT ON COLUMN observation.lat IS
'WGS84 latitude (from Digitransit).';
COMMENT ON COLUMN jore_stop.geom IS
'Stop POINT geometry in ETRS-TM35 coordinate system, generated from long & lat.';

--
-- Jore / Digitransit stations
--
-- These are modeled just like stops but their purpose is a bit different:
-- We may want to report stop results grouped by stations / terminals,
-- with station attributes, such as name, attached.
-- A station is never directly referred to as a stop by an HFP observation.
--

CREATE TABLE jore_station (
  LIKE jore_stop INCLUDING ALL
);
COMMENT ON TABLE jore_station IS
'Current HSL stations that may have stops belonging to them.';

--
-- Medians (= analysis results)
--
-- Most of the attributes could be calculated as view / materialized view,
-- but this would be tedious since the calculation is a multi-step process.
-- Therefore we use this table and procedures to populate and update it.
-- Note that only one analysis result per stop_id can be stored at a time.
--

CREATE TABLE stop_median (
  stop_id integer PRIMARY KEY,
  from_date date,
  to_date date,
  n_stop_known integer,
  n_stop_guessed integer,
  n_stop_null_near integer,
  dist_to_jore_point_m real,
  observation_route_dirs text[],
  result_class text,
  recommended_min_radius_m real,
  manual_acceptance_needed boolean,
  geom geometry(POINT, 3067)
);

CREATE INDEX ON stop_median USING GIST (geom);

COMMENT ON TABLE stop_median IS
'Median points of DOO / DOC point clusters by stop_id with materialized aggregate values.';
COMMENT ON COLUMN stop_median.from_date IS
'Min date (Finnish time) of the observations of the stop used in analysis.';
COMMENT ON COLUMN stop_median.to_date IS
'Max date (Finnish time) of the observations of the stop used in analysis.';
COMMENT ON COLUMN stop_median.n_stop_known IS
'N observations with stop_id from HFP/LIJ.';
COMMENT ON COLUMN stop_median.n_stop_guessed IS
'N observations with stop_id guessed by the analysis process.';
COMMENT ON COLUMN stop_median.n_stop_null_near IS
'N observations with NULL stop_id closer than STOP_NEAR_LIMIT_M to the stop.';
COMMENT ON COLUMN stop_median.dist_to_jore_point_m IS
'Distance (m) to the corresponding jore_stop(stop_id). ';
COMMENT ON COLUMN stop_median.observation_route_dirs IS
'Route-direction combinations that used the stop, according to observations.';
COMMENT ON COLUMN stop_median.result_class IS
'Result class for reporting.';
COMMENT ON COLUMN stop_median.recommended_min_radius_m IS
'Recommended minimum stop radius for Jore.';
COMMENT ON COLUMN stop_median.manual_acceptance_needed IS
'If true, the reported result needs manual inspection and acceptance.';
COMMENT ON COLUMN stop_median.geom IS
'Median POINT geometry in ETRS-TM35 coordinate system, geometric median from observations of the stop.';

CREATE TABLE percentile_radii (
  stop_id integer NOT NULL REFERENCES stop_median(stop_id) ON DELETE CASCADE,
  percentile real NOT NULL CHECK (percentile BETWEEN 0.0 AND 1.0),
  radius_m real,
  n_observations bigint,

  PRIMARY KEY (stop_id, percentile)
);

COMMENT ON TABLE percentile_radii IS
'Radii around "stop_median" containing a given percentage of "observation"
values ordered by their "dist_to_median_point_m".';
COMMENT ON COLUMN percentile_radii.stop_id IS
'Same as "stop_median"."stop_id".';
COMMENT ON COLUMN percentile_radii.percentile IS
'0.0 to 1.0, percentage of observations that the radius encloses.';
COMMENT ON COLUMN percentile_radii.radius_m IS
'Radius size in meters.';
COMMENT ON COLUMN percentile_radii.n_observations IS
'Number of observations that the radius encloses.';
