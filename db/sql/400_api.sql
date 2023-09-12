--
-- Views for REST API endpoints
--

CREATE SCHEMA api;


CREATE OR REPLACE VIEW api.view_as_original_hfp_event AS (
  SELECT
    point_timestamp AS tst,
    hfp_event AS event_type,
    route_id,
    direction_id,
    vehicle_operator_id as operator_id,
    observed_operator_id as oper,
    vehicle_number,
    transport_mode,
    oday,
    start,
    odo,
    spd,
    drst::int,
    loc,
    stop,
    ST_X(ST_Transform(geom, 4326)) AS long,
    ST_Y(ST_Transform(geom, 4326)) AS lat
  FROM hfp.hfp_point
);
COMMENT ON VIEW api.view_as_original_hfp_event IS 'View HFP points named like in original HFP data format.';


CREATE OR REPLACE VIEW api.view_as_original_apc_event AS (
  SELECT
    point_timestamp AS tst,
    received_at,
    vehicle_operator_id as operator_id,
    vehicle_number as veh,
    transport_mode as mode,
    route_id as route,
    direction_id as dir,
    oday,
    start,
    observed_operator_id as oper,
    stop,
    vehicle_load,
    vehicle_load_ratio,
    doors_data as door_counts,
    count_quality,
    ST_X(ST_Transform(geom, 4326)) AS long,
    ST_Y(ST_Transform(geom, 4326)) AS lat
  FROM apc.apc
);
COMMENT ON VIEW api.view_as_original_apc_event IS 'View APC data named like in original APC data format.';


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
     FROM jore.jore_stop
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc
);
COMMENT ON VIEW api.view_jore_stop_4326 IS
'Returns all jore_stops as GeoJSON features';

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
     FROM stopcorr.stop_median sm
     LEFT JOIN stopcorr.percentile_radii pr ON sm.stop_id = pr.stop_id
     GROUP BY sm.stop_id
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc
);
COMMENT ON VIEW api.view_stop_median_4326 IS
'Returns all stop_medians as GeoJSON features';

CREATE VIEW api.view_observation_4326 AS (
  WITH selected_cols AS (
    SELECT
      stop_id,
      stop_id_guessed,
      event,
      dist_to_jore_point_m,
      dist_to_median_point_m,
      ST_Transform(geom, 4326) as geometry
     FROM stopcorr.observation
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
    FROM jore.jore_stop js
    INNER JOIN LATERAL (
      SELECT * FROM stopcorr.observation AS ob
      WHERE ob.stop_id IS NULL AND
      ST_DWithin(ob.geom, js.geom, $2)
    ) as found_ob_stops
    ON true
    WHERE js.stop_id = $1
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc;
$$ LANGUAGE SQL STABLE;
COMMENT ON FUNCTION api.get_observations_with_null_stop_id_4326 IS
'Returns observations with no stop_id value, but located within search_distance_m from the given stop_id, as GeoJSON features';

CREATE OR REPLACE FUNCTION api.get_percentile_circles_with_stop_id(stop_id int)
RETURNS setof json as $$
  WITH selected_cols AS (
    SELECT * FROM stopcorr.view_percentile_circles pc WHERE pc.stop_id = $1
  )
  SELECT cast(ST_AsGeoJSON(sc.*) AS json)
  FROM selected_cols AS sc;
$$ LANGUAGE SQL STABLE;
COMMENT ON FUNCTION api.get_percentile_circles_with_stop_id IS
'Returns percentile circles around given stop_id as GeoJSON features';


CREATE VIEW api.view_assumed_monitored_vehicle_journey AS (
  SELECT
    route_id,
    direction_id,
    oday,
    -- Format: hhmmss, can implement 30h-transformed format later if requested.
    to_char("start", 'HH24:MI:SS') AS start_24h,
    -- Using a clearer name for Jubumera context.
    observed_operator_id AS journey_operator_id,
    transport_mode,
    vehicle_operator_id,
    vehicle_number,
    min_timestamp,
    max_timestamp,
    arr_count,
    modified_at
  FROM hfp.assumed_monitored_vehicle_journey
);
COMMENT ON VIEW api.view_assumed_monitored_vehicle_journey IS
'Returns all monitored vehicle journeys.';
