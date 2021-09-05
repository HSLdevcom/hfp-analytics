--
-- Views for reporting.
--

CREATE VIEW view_median_report_fields AS (
  WITH
    radii_info AS (
      SELECT
        stop_id,
        string_agg(
          round((percentile*100)::numeric) || ' % ' || round(radius_m::numeric) || ' m',
           ', ' ORDER BY percentile) AS radii_info
      FROM percentile_radii
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
  FROM stop_median AS sm
  INNER JOIN radii_info AS ri
    ON (sm.stop_id = ri.stop_id)
  LEFT JOIN jore_stop AS js
    ON (sm.stop_id = js.stop_id)
);

COMMENT ON VIEW view_median_report_fields IS
'Text field contents by "stop_median"."stop_id" for report slides.';

CREATE VIEW view_percentile_circle AS (
  SELECT
    pr.stop_id,
    pr.percentile,
    pr.radius_m,
    pr.n_observations,
    ST_Buffer(sm.geom, pr.radius_m) AS geom
  FROM percentile_radii AS pr
  INNER JOIN stop_median AS sm
    ON (pr.stop_id = sm.stop_id)
);

COMMENT ON VIEW view_percentile_circle IS
'Percentile circles with "radius_m" radii around "stop_median" points.';

CREATE VIEW view_report_viewport AS (
  WITH
    max_percentile_radii AS (
      SELECT stop_id, max(radius_m) AS radius_m
      FROM percentile_radii
      GROUP BY stop_id
    )
  SELECT
    sm.stop_id,
    ST_Envelope(
      ST_Transform(
        ST_Buffer(
          sm.geom,
          greatest(42.0, sm.dist_to_jore_point_m, mpr.radius_m) + 10.0
        ),
      4326)
    ) AS geom
  FROM stop_median AS sm
  INNER JOIN max_percentile_radii AS mpr
    ON (sm.stop_id = mpr.stop_id)
);

COMMENT ON VIEW view_report_viewport IS
'Viewports for reporting stop medians, containing the median and Jore stop point
and the highest available percentile of observations around the stop.';
