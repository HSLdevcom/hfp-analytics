# Call analysis functions in the db.

import psycopg2
import os
from psycopg2 import sql
from stopcorr.utils import get_conn_params

def main():
    stop_near_limit_m = os.getenv('STOP_NEAR_LIMIT_M')
    if stop_near_limit_m is None:
        stop_near_limit_m = 50.0
        print(f'STOP_NEAR_LIMIT_M not set, falling back to default value {stop_near_limit_m}')

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
    finally:
        conn.close()

if __name__ == '__main__':
    main()
