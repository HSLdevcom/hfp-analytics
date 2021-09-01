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
