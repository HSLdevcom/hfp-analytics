# Call analysis functions in the db.

import psycopg2
from psycopg2 import sql
from .stopcorr.utils import get_conn_params
from .stopcorr.utils import env_with_default
from .stopcorr.utils import comma_separated_floats_to_list
from .stopcorr.utils import comma_separated_integers_to_list

def main():
    stop_near_limit_m = env_with_default('STOP_NEAR_LIMIT_M', 50.0)
    min_observations_per_stop = env_with_default('MIN_OBSERVATIONS_PER_STOP', 10)
    max_null_stop_dist_m = env_with_default('MAX_NULL_STOP_DIST_M', 100.0)
    radius_percentiles_str = env_with_default('RADIUS_PERCENTILES', '0.5,0.75,0.9,0.95')
    radius_percentiles = comma_separated_floats_to_list(radius_percentiles_str)

    min_radius_percentiles_to_sum_str = env_with_default('MIN_RADIUS_PERCENTILES_TO_SUM', '0.5,0.95')
    min_radius_percentiles_to_sum = comma_separated_floats_to_list(min_radius_percentiles_to_sum_str)
    default_min_radius_m = env_with_default('DEFAULT_MIN_RADIUS_M', 20.0)
    manual_acceptance_min_radius_m = env_with_default('MANUAL_ACCEPTANCE_MIN_RADIUS_M', 40.0)
    large_scatter_percentile = env_with_default('LARGE_SCATTER_PERCENTILE', 0.9)
    large_scatter_radius_m = env_with_default('LARGE_SCATTER_RADIUS_M', 10.0)
    large_jore_dist_m = env_with_default('LARGE_JORE_DIST_M', 25.0)
    stop_guessed_percentage = env_with_default('STOP_GUESSED_PERCENTAGE', 0.05)
    terminal_ids_str = env_with_default('TERMINAL_IDS', '1000001,1000015,2000002,2000003,2000212,4000011')
    terminal_ids = comma_separated_integers_to_list(terminal_ids_str)

    conn = psycopg2.connect(**get_conn_params())

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM guess_missing_stop_ids(%s)',
                            (stop_near_limit_m, ))
                print(f'{cur.fetchone()[0]} observations updated with guessed stop_id')

                cur.execute('SELECT stop_id FROM observed_stop_not_in_jore_stop')
                res = [str(x[0]) for x in cur.fetchall()]
                n_stops = len(res)
                if n_stops > 10:
                    print(f'{n_stops} stop_id values in "observation" not found in "jore_stop"')
                elif n_stops > 0:
                    stops_str = ', '.join(res)
                    print(f'stop_id values in "observation" not found in "jore_stop": {stops_str}')

                cur.execute('SELECT * FROM calculate_jore_distances()')
                print(f'{cur.fetchone()[0]} observations updated with dist_to_jore_point_m')

                cur.execute('WITH deleted AS (DELETE FROM stop_median RETURNING 1)\
                            SELECT count(*) FROM deleted')
                print(f'{cur.fetchone()[0]} rows deleted from "stop_median"')

                cur.execute('SELECT * FROM calculate_medians(%s, %s)',
                            (min_observations_per_stop, max_null_stop_dist_m))
                print(f'{cur.fetchone()[0]} rows inserted into "stop_median"')

                cur.execute('SELECT * FROM calculate_median_distances()')
                print(f'{cur.fetchone()[0]} observations updated with dist_to_median_point_m')

                cur.execute('SELECT * FROM calculate_percentile_radii(%s)',
                            (radius_percentiles, ))
                print(f'{cur.fetchone()[0]} "percentile_radii" created using percentiles {radius_percentiles_str}')

                cur.execute('CALL classify_medians(%s, %s, %s, %s, %s, %s, %s, %s)',
                            (min_radius_percentiles_to_sum,
                             default_min_radius_m,
                             manual_acceptance_min_radius_m,
                             large_scatter_percentile,
                             large_scatter_radius_m,
                             large_jore_dist_m,
                             stop_guessed_percentage,
                             terminal_ids)
                            )
                cur.execute('SELECT count(*) FROM stop_median WHERE result_class IS NOT NULL')
                print(f'{cur.fetchone()[0]} "stop_median" updated with "result_class", "recommended_min_radius_m" and "manual_acceptance_needed"')

    finally:
        conn.close()

if __name__ == '__main__':
    main()
