--
-- Views for reporting.
--

CREATE VIEW stopcorr.view_median_report_fields AS (
  WITH
    radii_info AS (
      SELECT
        stop_id,
        string_agg(
          round((percentile*100)::numeric) || ' % ' || round(radius_m::numeric) || ' m',
           ', ' ORDER BY percentile) AS radii_info
      FROM stopcorr.percentile_radii
      GROUP BY stop_id
    )
  SELECT
    sm.stop_id AS stop_id,
    sm.stop_id || ' ' || coalesce(js.stop_name, '(puuttuu)') AS title,
    sm.result_class,
    (round(ST_Y(ST_Transform(sm.geom, 4326))::numeric, 6) || ', ' ||
      round(ST_X(ST_Transform(sm.geom, 4326))::numeric, 6)) AS median_coordinates,
    round(sm.dist_to_jore_point_m) || ' m' AS dist_to_jore,
    round(sm.recommended_min_radius_m) || ' m' AS recomm_radius,
    coalesce(sm.n_stop_known::text, '0') AS n_stop_known,
    ri.radii_info AS radii_info,
    coalesce(sm.n_stop_guessed::text, '0') AS n_stop_guessed,
    coalesce(sm.n_stop_null_near::text, '0') AS n_stop_null_near,
    array_to_string(sm.observation_route_dirs, ', ') AS observation_route_dirs,
    to_char(sm.from_date, 'DD.MM.YYYY') || '-' ||
      to_char(sm.to_date, 'DD.MM.YYYY') AS observation_date_range,
    coalesce(to_char(js.date_imported, 'DD.MM.YYYY'), '(puuttuu)') AS stop_import_date,
    'https://reittiloki.hsl.fi/?date=' ||
      to_char(coalesce(js.date_imported, CURRENT_DATE), 'YYYY-MM-DD') ||
      '&stop=' || sm.stop_id AS transitlog_url
  FROM stopcorr.stop_median AS sm
  INNER JOIN radii_info AS ri
    ON (sm.stop_id = ri.stop_id)
  LEFT JOIN jore.jore_stop AS js
    ON (sm.stop_id = js.stop_id)
);

COMMENT ON VIEW stopcorr.view_median_report_fields IS
'Text field contents by "stop_median"."stop_id" for report slides.';


CREATE VIEW stopcorr.view_report_viewport AS (
  WITH
    max_percentile_radii AS (
      SELECT stop_id, max(radius_m) AS radius_m
      FROM stopcorr.percentile_radii
      GROUP BY stop_id
    )
  SELECT
    sm.stop_id,
    ST_Envelope(
      ST_Transform(
        ST_Buffer(
          sm.geom,
          greatest(50.0, sm.dist_to_jore_point_m, mpr.radius_m) + 10.0
        ),
      4326)
    ) AS geom
  FROM stopcorr.stop_median AS sm
  INNER JOIN max_percentile_radii AS mpr
    ON (sm.stop_id = mpr.stop_id)
);

COMMENT ON VIEW stopcorr.view_report_viewport IS
'Viewports for reporting stop medians, containing the median and Jore stop point
and the highest available percentile of observations around the stop.';

CREATE VIEW stopcorr.view_median_to_jore_lines AS (
  SELECT
    sm.stop_id,
    ST_MakeLine(sm.geom, js.geom) AS geom
  FROM stopcorr.stop_median AS sm
  INNER JOIN jore.jore_stop AS js
    ON (sm.stop_id = js.stop_id)
);

COMMENT ON VIEW stopcorr.view_median_to_jore_lines IS
'LINESTRING geometries from "stop_median" to "jore_stop" by stop_id.';
