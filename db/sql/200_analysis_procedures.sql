--
-- Guessing missing stop_ids afterwards
--
-- In some cases LIJ/HFP has not assigned a stop_id to door messages
-- although they clearly occur at a stop that belongs to the route and direction
-- the vehicle is operating on.
-- We can try to improve on the data in these cases by populating the stop_id
-- of an observation, if
-- 1) the observation is closer than STOP_NEAR_LIMIT_M meters to a stop point, and
-- 2) if the route & dir of the observation is found in the route-dirs of the stop
--    given by Digitransit.
-- Should there be multiple candidate stops, the closest one is chosen.
--
-- This function does the above and sets stop_id_guessed = true for the updated rows.
-- Note that the procedure is done for the whole public.observation table:
-- e.g. if there are old observations left with NULL stop_id
-- due to a lower STOP_NEAR_LIMIT_M value, and now we use a higher value letting
-- further away observations to be included, some old observations may be
-- assigned a guessed stop_id.
--

CREATE FUNCTION guess_missing_stop_ids(stop_near_limit_m double precision)
RETURNS bigint
VOLATILE
LANGUAGE SQL
AS $func$
  WITH
    guessed_stops AS (
      SELECT
        ob.tst, ob.event, ob.oper, ob.veh,
        found_stop.stop_id
      FROM observation AS ob
      INNER JOIN LATERAL (
        SELECT js.stop_id, ST_Distance(ob.geom, js.geom)
        FROM jore_stop AS js
        WHERE ST_DWithin(ob.geom, js.geom, $1)
          AND (ob.route || '-' || ob.dir) = ANY(js.route_dirs_via_stop)
        ORDER BY ST_Distance(ob.geom, js.geom)
        LIMIT 1
      ) AS found_stop
      ON true
      WHERE ob.stop_id IS NULL
    ),
    updated AS (
      UPDATE observation AS ob
      SET
        stop_id = gs.stop_id,
        stop_id_guessed = true
      FROM guessed_stops AS gs
      WHERE ob.tst = gs.tst
        AND ob.event = gs.event
        AND ob.oper = gs.oper
        AND ob.veh = gs.veh
      RETURNING 1
    )
  SELECT count(*) FROM updated;
$func$;

COMMENT ON FUNCTION guess_missing_stop_ids IS
'For observations with NULL stop_id, searches for stops that are used by the
route+dir of the observation and are within the given distance from the observation;
if found, sets the closest stop as the stop_id and stop_id_guessed to true.
Returns the number of observations updated.';

--
-- Report stop_id values that are missing from jore_stop.
--

CREATE VIEW observed_stop_not_in_jore_stop AS (
  SELECT DISTINCT stop_id
  FROM observation
  WHERE stop_id IS NOT NULL
    AND stop_id NOT IN (SELECT DISTINCT stop_id FROM jore_stop)
  ORDER BY stop_id
);

COMMENT ON VIEW observed_stop_not_in_jore_stop IS
'Returns unique stop_id values from "observation" that are not found in "jore_stop".';

--
-- Calculate distances between observations (with stop_id) and Jore stops.
--

CREATE FUNCTION calculate_jore_distances()
RETURNS bigint
VOLATILE
LANGUAGE SQL
AS $func$
  WITH updated AS (
    UPDATE observation AS ob
    SET dist_to_jore_point_m = ST_Distance(ob.geom, js.geom)
    FROM jore_stop AS js
    WHERE ob.stop_id = js.stop_id
    RETURNING 1
  )
  SELECT count(*) FROM updated;
$func$;

COMMENT ON FUNCTION calculate_jore_distances IS
'Populates observation.dist_to_jore_point_m with the distance from observation
to jore_stop by stop_id.';

--
-- Calculate medians of observation clusters by stop_id.
--

CREATE FUNCTION calculate_medians(
  min_observations_per_stop integer,
  max_null_stop_dist_m      double precision
) RETURNS bigint
VOLATILE
LANGUAGE SQL
AS $func$
WITH
  medians AS (
    SELECT
      stop_id,
      (min(tst) AT TIME ZONE 'Europe/Helsinki')::date               AS from_date,
      (max(tst) AT TIME ZONE 'Europe/Helsinki')::date               AS to_date,
      count(*) filter(WHERE stop_id_guessed IS false)               AS n_stop_known,
      count(*) filter(WHERE stop_id_guessed IS true)                AS n_stop_guessed,
      array_agg(
        DISTINCT (route || '-' || dir) ORDER BY (route || '-' || dir)
        )                                                           AS observation_route_dirs,
      ST_GeometricMedian( ST_Union(geom) )                          AS geom
    FROM observation
    WHERE stop_id IS NOT NULL
      AND geom IS NOT NULL
    GROUP BY stop_id
    HAVING count(*) >= $1
  ),
  near_nulls_by_median AS (
    SELECT
      m.stop_id,
      count(*) AS n_stop_null_near
    FROM medians AS m
    INNER JOIN observation AS o
      ON ST_DWithin(m.geom, o.geom, $2)
    WHERE o.stop_id IS NULL
    GROUP BY m.stop_id
  ),
  inserted AS (
    INSERT INTO stop_median (
      stop_id,
      from_date,
      to_date,
      n_stop_known,
      n_stop_guessed,
      n_stop_null_near,
      dist_to_jore_point_m,
      observation_route_dirs,
      geom
    )
    SELECT
      md.stop_id,
      md.from_date,
      md.to_date,
      md.n_stop_known,
      md.n_stop_guessed,
      nn.n_stop_null_near,
      ST_Distance(md.geom, js.geom),
      md.observation_route_dirs,
      md.geom
    FROM medians AS md
    LEFT JOIN jore_stop AS js
      ON (md.stop_id = js.stop_id)
    LEFT JOIN near_nulls_by_median AS nn
      ON (md.stop_id = nn.stop_id)
    RETURNING 1
  )
SELECT count(*) FROM inserted;
$func$;

COMMENT ON FUNCTION calculate_medians IS
'Populates stop_median with ST_GeometricMedian and related aggregates from each
stop_id that has at least "min_observations_per_stop" rows in "observation".';

--
-- Calculate distances between observations and medians by stop_id.
--

CREATE FUNCTION calculate_median_distances()
RETURNS bigint
VOLATILE
LANGUAGE SQL
AS $func$
  WITH updated AS (
    UPDATE observation AS ob
    SET dist_to_median_point_m = ST_Distance(ob.geom, sm.geom)
    FROM stop_median AS sm
    WHERE ob.stop_id = sm.stop_id
    RETURNING 1
  )
  SELECT count(*) FROM updated;
$func$;

COMMENT ON FUNCTION calculate_median_distances IS
'Populates observation.dist_to_median_point_m with the distance
from observation to stop_median by stop_id.';

--
-- Calculate percentile radii around the median points.
--

CREATE FUNCTION calculate_percentile_radii(percentiles real[])
RETURNS bigint
VOLATILE
LANGUAGE SQL
AS $func$
  WITH
    radii AS (
      SELECT
        ob.stop_id,
        pe.pe AS percentile,
        percentile_cont(pe.pe) WITHIN GROUP (ORDER BY ob.dist_to_median_point_m) AS radius_m
      FROM observation AS ob
      INNER JOIN unnest($1) AS pe
        ON true
      INNER JOIN stop_median AS sm -- Just to avoid records missing from stop_median
        ON (ob.stop_id = sm.stop_id)
      WHERE ob.stop_id IS NOT NULL
      GROUP BY ob.stop_id, pe.pe
    ),
    counts AS (
      SELECT
        rd.stop_id,
        rd.percentile,
        count(*) FILTER (WHERE ob.dist_to_median_point_m <= rd.radius_m) AS n_observations
      FROM radii AS rd
      INNER JOIN observation AS ob
        ON (rd.stop_id = ob.stop_id)
      GROUP BY rd.stop_id, rd.percentile
    ),
    inserted AS (
      INSERT INTO percentile_radii (stop_id, percentile, radius_m, n_observations)
      SELECT rd.stop_id, rd.percentile, rd.radius_m, cn.n_observations
      FROM radii AS rd
      INNER JOIN counts AS cn
        ON (rd.stop_id = cn.stop_id AND rd.percentile = cn.percentile)
      WHERE rd.radius_m IS NOT NULL
      ORDER BY stop_id, percentile
      RETURNING 1
    )
  SELECT count(*) FROM inserted;
$func$;

COMMENT ON FUNCTION calculate_percentile_radii IS
'Populates "percentile_radii" with radii enclosing "observation" points
around "stop_median" points at given percentages (from 0.0 to 1.0).';

--
-- Classification of medians.
--
-- Note that the classes are not exclusive, however only the class applied last
-- will remain valid.
--

CREATE PROCEDURE classify_medians(
  min_radius_percentiles_to_sum real[],
  default_min_radius_m real,
  manual_acceptance_min_radius_m real,
  large_scatter_percentile real,
  large_scatter_radius_m real,
  large_jore_dist_m real,
  stop_guessed_percentage real,
  terminal_ids integer[]
)
LANGUAGE PLPGSQL
AS $proc$
BEGIN

  UPDATE stop_median AS sm SET
    result_class = NULL,
    recommended_min_radius_m = NULL,
    manual_acceptance_needed = NULL;

  UPDATE stop_median AS sm
  SET
    recommended_min_radius_m = greatest(rd.selected_radius_sum, default_min_radius_m),
    manual_acceptance_needed = (rd.selected_radius_sum > manual_acceptance_min_radius_m)
  FROM (
      SELECT stop_id, sum(radius_m) AS selected_radius_sum
      FROM percentile_radii
      WHERE percentile = ANY (min_radius_percentiles_to_sum)
      GROUP BY stop_id
    ) AS rd
  WHERE sm.stop_id = rd.stop_id;

  UPDATE stop_median AS sm
  SET result_class = 'Korjaa säde / sijainti'
  FROM (SELECT stop_id, radius_m FROM percentile_radii WHERE percentile = large_scatter_percentile) AS rd
  WHERE sm.stop_id = rd.stop_id
    AND sm.dist_to_jore_point_m >= large_jore_dist_m
    AND rd.radius_m <= large_scatter_radius_m;

  UPDATE stop_median AS sm
  SET result_class = 'Tarkista (suuri hajonta)'
  FROM (SELECT stop_id, radius_m FROM percentile_radii WHERE percentile = large_scatter_percentile) AS rd
  WHERE sm.stop_id = rd.stop_id
    AND rd.radius_m > large_scatter_radius_m;

  UPDATE stop_median SET result_class = 'Tarkista (paljon jälkikohdistettuja)'
  WHERE (n_stop_guessed::real / (n_stop_known+n_stop_guessed)::real > stop_guessed_percentage);

  UPDATE stop_median SET result_class = 'Tarkista (terminaali)'
  WHERE stop_id IN (
      SELECT stop_id FROM jore_stop
      WHERE parent_station = ANY (terminal_ids)
    );

  UPDATE stop_median SET result_class = 'Tarkista (ratikka)'
  WHERE stop_id IN (SELECT stop_id FROM jore_stop WHERE stop_mode = 'TRAM');

  UPDATE stop_median SET result_class = 'Jore-pysäkki puuttuu'
  WHERE stop_id NOT IN (SELECT stop_id FROM jore_stop);

END;
$proc$;

COMMENT ON PROCEDURE classify_medians IS
'Updates result_class, recommended_min_radius_m and manual_acceptance_needed
for "stop_median".';
