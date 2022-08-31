--
-- Views for REST API endpoints
--

CREATE SCHEMA api;

CREATE VIEW api.view_jore_stop_4326 AS (
  WITH selected_cols AS (
    SELECT
      stop_id,
      stop_code,
      stop_name,
      parent_station,
      stop_mode,
      route_dirs_via_stop,
      date_imported,
      ST_Transform(geom, 4326) as geometry
     FROM public.jore_stop
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc
);
COMMENT ON VIEW api.view_jore_stop_4326 IS
'Returns jore_stops as GeoJSON features';

CREATE VIEW api.view_stop_median_4326 AS (
  WITH selected_cols AS (
    SELECT
      sm.stop_id,
      sm.from_date,
      sm.n_stop_known,
      sm.n_stop_guessed,
      sm.n_stop_null_near,
      sm.dist_to_jore_point_m,
      sm.observation_route_dirs,
      sm.result_class,
      sm.recommended_min_radius_m,
      sm.manual_acceptance_needed,
      json_agg(
        json_build_object(
          'percentile', pr.percentile,
          'radius_m', pr.radius_m,
          'n_observations', pr.n_observations
        )
      ) as percentile_radii_list,
      ST_Transform(sm.geom, 4326) as geometry
     FROM public.stop_median sm
     LEFT JOIN percentile_radii pr ON sm.stop_id = pr.stop_id
     GROUP BY sm.stop_id
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc
);
COMMENT ON VIEW api.view_stop_median_4326 IS
'Returns stop_medians as GeoJSON features';

CREATE VIEW api.view_observation_4326 AS (
  WITH selected_cols AS (
    SELECT
      stop_id,
      stop_id_guessed,
      event,
      dist_to_jore_point_m,
      dist_to_median_point_m,
      ST_Transform(geom, 4326) as geometry
     FROM public.observation
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc
);
COMMENT ON VIEW api.view_observation_4326 IS
'Returns all observations as GeoJSON features';

CREATE OR REPLACE FUNCTION api.get_observations_with_null_stop_id_4326(stop_id int, search_distance_m int default 100)
RETURNS setof json as $$
  WITH selected_cols AS (
    SELECT
      found_ob_stops.stop_id,
      found_ob_stops.stop_id_guessed,
      found_ob_stops.event,
      found_ob_stops.dist_to_jore_point_m,
      found_ob_stops.dist_to_median_point_m,
      ST_Transform(found_ob_stops.geom, 4326) as geometry
    FROM jore_stop js
    INNER JOIN LATERAL (
      SELECT * FROM observation AS ob
      WHERE ob.stop_id IS NULL AND
      ST_DWithin(ob.geom, js.geom, $2)
    ) as found_ob_stops
    ON true
    WHERE js.stop_id = $1
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc;
$$ LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION api.get_observations_with_null_stop_id_4326 IS
'Returns observations with null stop_id as GeoJSON features';

CREATE OR REPLACE FUNCTION api.get_percentile_circles_with_stop_id(stop_id int)
RETURNS setof json as $$
  WITH selected_cols AS (
    SELECT * FROM stopcorr.view_percentile_circles pc WHERE pc.stop_id = $1
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc;
$$ LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION api.get_percentile_circles_with_stop_id IS
'Returns percentile circles with stop_id as GeoJSON features';
