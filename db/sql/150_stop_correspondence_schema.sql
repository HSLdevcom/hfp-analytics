---
--- Tables for stop correspondence analysis.
---
CREATE SCHEMA stopcorr;
COMMENT ON SCHEMA stopcorr IS
'For stop correspondence analysis';

--
-- HFP observations (DOC / DOO events)
--
CREATE TABLE stopcorr.observation (
  tst                     timestamptz   NOT NULL,
  event                   text          NOT NULL,
  oper                    integer       NOT NULL,
  veh                     integer       NOT NULL,
  route                   text,
  dir                     smallint,
  oday                    date,
  start                   time,
  stop_id                 integer,
  stop_id_guessed         boolean,
  dist_to_jore_point_m    real,
  dist_to_median_point_m  real,
  geom                    geometry(POINT, 3067),

  PRIMARY KEY (tst, event, oper, veh)
);

CREATE INDEX observation_stop_idx ON stopcorr.observation (stop_id);
CREATE INDEX ON stopcorr.observation USING GIST (geom);

COMMENT ON TABLE stopcorr.observation IS
'DOO and DOC events imported from HFP. See Digitransit doc for the first columns.';
COMMENT ON COLUMN stopcorr.observation.stop_id IS
'Long stop id from Jore/Digitransit, same as HFP "stop".';
COMMENT ON COLUMN stopcorr.observation.stop_id_guessed IS
'If false, stop_id comes from HFP/LIJ. If true, it has been "guessed" afterwards by the analysis process.';
COMMENT ON COLUMN stopcorr.observation.dist_to_jore_point_m IS
'Distance (m) to the corresponding jore_stop(stop_id).';
COMMENT ON COLUMN stopcorr.observation.dist_to_median_point_m IS
'Distance (m) to the corresponding stop_median(stop_id).';
COMMENT ON COLUMN stopcorr.observation.geom IS
'Observation POINT geometry in ETRS-TM35 coordinate system, generated from long & lat.';

CREATE OR REPLACE FUNCTION stopcorr.refresh_observation() RETURNS bigint AS $func$
DECLARE
  n_inserted bigint;
BEGIN
  DELETE FROM stopcorr.observation;
  WITH ins AS (
    INSERT INTO stopcorr.observation (tst, event, oper, veh, route, dir, oday, start, stop_id, geom)
    SELECT
      point_timestamp,
      hfp_event,
      observed_operator_id,
      vehicle_number,
      route_id,
      direction_id,
      oday,
      start,
      stop,
      geom
    FROM hfp.hfp_point
    WHERE
      hfp_event IN ('DOO', 'DOC') AND geom IS NOT NULL
    ON CONFLICT DO NOTHING
    RETURNING 1
  )
  SELECT INTO n_inserted count(*) FROM ins;
  RETURN n_inserted;
END;
$func$
LANGUAGE plpgsql VOLATILE;


--
-- Medians (= analysis results)
--
-- Most of the attributes could be calculated as view / materialized view,
-- but this would be tedious since the calculation is a multi-step process.
-- Therefore we use this table and procedures to populate and update it.
-- Note that only one analysis result per stop_id can be stored at a time.
--

CREATE TABLE stopcorr.stop_median (
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

CREATE INDEX ON stopcorr.stop_median USING GIST (geom);

COMMENT ON TABLE stopcorr.stop_median IS
'Median points of DOO / DOC point clusters by stop_id with materialized aggregate values.';
COMMENT ON COLUMN stopcorr.stop_median.from_date IS
'Min date (Finnish time) of the observations of the stop used in analysis.';
COMMENT ON COLUMN stopcorr.stop_median.to_date IS
'Max date (Finnish time) of the observations of the stop used in analysis.';
COMMENT ON COLUMN stopcorr.stop_median.n_stop_known IS
'N observations with stop_id from HFP/LIJ.';
COMMENT ON COLUMN stopcorr.stop_median.n_stop_guessed IS
'N observations with stop_id guessed by the analysis process.';
COMMENT ON COLUMN stopcorr.stop_median.n_stop_null_near IS
'N observations with NULL stop_id closer than STOP_NEAR_LIMIT_M to the stop.';
COMMENT ON COLUMN stopcorr.stop_median.dist_to_jore_point_m IS
'Distance (m) to the corresponding jore_stop(stop_id). ';
COMMENT ON COLUMN stopcorr.stop_median.observation_route_dirs IS
'Route-direction combinations that used the stop, according to observations.';
COMMENT ON COLUMN stopcorr.stop_median.result_class IS
'Result class for reporting.';
COMMENT ON COLUMN stopcorr.stop_median.recommended_min_radius_m IS
'Recommended minimum stop radius for Jore.';
COMMENT ON COLUMN stopcorr.stop_median.manual_acceptance_needed IS
'If true, the reported result needs manual inspection and acceptance.';
COMMENT ON COLUMN stopcorr.stop_median.geom IS
'Median POINT geometry in ETRS-TM35 coordinate system, geometric median from observations of the stop.';

CREATE TABLE stopcorr.percentile_radii (
  stop_id integer NOT NULL REFERENCES stopcorr.stop_median(stop_id) ON DELETE CASCADE,
  percentile real NOT NULL CHECK (percentile BETWEEN 0.0 AND 1.0),
  radius_m real,
  n_observations bigint,

  PRIMARY KEY (stop_id, percentile)
);

COMMENT ON TABLE stopcorr.percentile_radii IS
'Radii around "stop_median" containing a given percentage of "observation"
values ordered by their "dist_to_median_point_m".';
COMMENT ON COLUMN stopcorr.percentile_radii.stop_id IS
'Same as "stop_median"."stop_id".';
COMMENT ON COLUMN stopcorr.percentile_radii.percentile IS
'0.0 to 1.0, percentage of observations that the radius encloses.';
COMMENT ON COLUMN stopcorr.percentile_radii.radius_m IS
'Radius size in meters.';
COMMENT ON COLUMN stopcorr.percentile_radii.n_observations IS
'Number of observations that the radius encloses.';

CREATE VIEW stopcorr.view_percentile_circles AS (
  SELECT
    pr.stop_id,
    pr.percentile,
    pr.radius_m,
    pr.n_observations,
    ST_Buffer(sm.geom, pr.radius_m, 'quad_segs=16') AS geom
  FROM stopcorr.percentile_radii AS pr
  INNER JOIN stopcorr.stop_median AS sm
    ON (pr.stop_id = sm.stop_id)
);
COMMENT ON VIEW stopcorr.view_percentile_circles IS
'Percentile circles with "radius_m" radii around "stop_median" points.';