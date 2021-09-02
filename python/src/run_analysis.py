# Call analysis functions in the db.

import psycopg2
from psycopg2 import sql
from stopcorr.utils import get_conn_params
from stopcorr.utils import env_with_default
from stopcorr.utils import comma_separated_floats_to_list

def main():
    stop_near_limit_m = env_with_default('STOP_NEAR_LIMIT_M', 50.0)
    min_observations_per_stop = env_with_default('MIN_OBSERVATIONS_PER_STOP', 10)
    max_null_stop_dist_m = env_with_default('MAX_NULL_STOP_DIST_M', 100.0)
    radius_percentiles_str = env_with_default('RADIUS_PERCENTILES', '0.5,0.75,0.9,0.95')
    radius_percentiles = comma_separated_floats_to_list(radius_percentiles_str)

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

    finally:
        conn.close()

if __name__ == '__main__':
    main()
