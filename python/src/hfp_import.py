# Call db server-side COPY to import HFP events from csv.

import psycopg2
import os
from datetime import datetime

def main():
    starttime = datetime.now()
    print(f'[{starttime}] Importing HFP events to database')

    conn_params = dict(
        dbname = os.getenv('POSTGRES_DB'),
        user = os.getenv('POSTGRES_USER'),
        password = os.getenv('POSTGRES_PASSWORD'),
        host = 'db',
        port = 5432
    )

    conn = psycopg2.connect(**conn_params)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMPORARY TABLE _import ( \
                    LIKE observation) \
                    ON COMMIT DROP")
                cur.execute(
                    "COPY _import (\
                    tst,event,oper,veh,route,dir,oday,start,stop_id,long,lat) \
                    FROM PROGRAM 'gzip -cd /import/hfp.csv.gz' \
                    CSV HEADER"
                )
                cur.execute("SELECT count(1) FROM _import")
                print(f'{cur.fetchone()[0]} events read')
                cur.execute(
                    "WITH inserted AS ( \
                    INSERT INTO observation ( \
                    tst,event,oper,veh,route,dir,oday,start,stop_id,long,lat) \
                    SELECT tst,event,oper,veh,route,dir,oday,start,stop_id,long,lat \
                    FROM _import \
                    ON CONFLICT DO NOTHING \
                    RETURNING 1 ) \
                    SELECT count(1) FROM inserted"
                )
                print(f'{cur.fetchone()[0]} events imported')
                endtime = datetime.now()
                print(f'[{endtime}] HFP events imported in {endtime-starttime}')
    finally:
        conn.close()

if __name__ == '__main__':
    main()
