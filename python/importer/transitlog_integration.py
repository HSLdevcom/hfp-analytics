"""Import data from Transitlog DB to HFP Analytics DB."""

import logging
import os
import psycopg2
import sys
from psycopg2 import sql
from datetime import date

log = logging.getLogger(__name__)

# These SQL templates are used for reading from Transitlog DB.

ROUTE_SQL = "SELECT route_id, direction, date_begin, date_end, type, date_modified, date_imported \
FROM jore.route \
WHERE daterange(date_begin, date_end) && daterange(%(fromdate)s, %(todate)s)
"

ROUTE_SEGMENT_SQL = "SELECT route_id, direction, date_begin, date_end, stop_id, stop_index, timing_stop_type, date_modified, date_imported \
FROM jore.route_segment \
WHERE daterange(date_begin, date_end) && daterange(%(fromdate)s, %(todate)s)
"

EXCEPTION_DAYS_CALENDAR_SQL = "SELECT date_in_effect, exception_day_type, day_type, exclusive, date_imported \
FROM jore.exception_days_calendar \
WHERE date_in_effect <@ daterange(%(fromdate)s, %(todate)s)
"

REPLACEMENT_DAYS_CALENDAR_SQL = "SELECT date_in_effect, scope, replacing_day_type, day_type, time_begin, time_end, date_imported \
FROM jore.replacement_days_calendar \
WHERE date_in_effect <@ daterange(%(fromdate)s, %(todate)s)
"

DEPARTURE_SQL = "SELECT \
    route_id, direction, date_begin, date_end, hours, minutes, stop_id, day_type, extra_departure, \
    is_next_day, arrival_is_next_day, arrival_hours, arrival_minutes, operator_id, date_imported \
FROM jore.departure \
WHERE daterange(date_begin, date_end) && daterange(%(fromdate)s, %(todate)s)
"

def copy_dated_sql_to_csv(
    cur: psycopg2.cursor,
    sql_template: str,
    fromdate: date,
    todate: date,
    csv_path: str
) -> None:
    """Copy result of SQL query, with date params, into csv file."""
    sql_template = f'COPY ({sql_template}) TO STDOUT WITH CSV HEADER'
    sql_query = sql.SQL(sql_template).format(fromdate=fromdate, todate=todate)
    with open(csv_path, 'w') as fobj:
        cur.copy_expert(sql=sql_query, file=fobj)

def copy_all_from_transitlog(
    transitlog_connstr: str, 
    fromdate: date, 
    todate: date,
    target_directory: str
) -> None:
    """FOR DEV USE.
    Import all required Jore data concerning the given date range
    from Transitlog DB to csv files that can be read into local dev db."""
    conn = None
    try:
        conn = psycopg2.connect(transitlog_connstr)
        if fromdate > todate:
            raise ValueError(f'{fromdate=} must not be greater than {todate=}.')
        if not os.path.exists(target_directory):
            raise OSError(f'{target_directory} does not exist, please create it first.')
        with conn.cursor() as cur:
            copy_dated_sql_to_csv(
                cur=cur, 
                sql_template=ROUTE_SQL, 
                fromdate=fromdate, 
                todate=todate,
                csv_path=os.path.join(target_directory, 'route.csv')
            )
            copy_dated_sql_to_csv(
                cur=cur, 
                sql_template=ROUTE_SEGMENT_SQL, 
                fromdate=fromdate, 
                todate=todate,
                csv_path=os.path.join(target_directory, 'route_segment.csv')
            )
            copy_dated_sql_to_csv(
                cur=cur, 
                sql_template=EXCEPTION_DAYS_CALENDAR_SQL, 
                fromdate=fromdate, 
                todate=todate,
                csv_path=os.path.join(target_directory, 'exception_days_calendar.csv')
            )
            copy_dated_sql_to_csv(
                cur=cur, 
                sql_template=REPLACEMENT_DAYS_CALENDAR_SQL, 
                fromdate=fromdate, 
                todate=todate,
                csv_path=os.path.join(target_directory, 'replacement_days_calendar.csv')
            )
            copy_dated_sql_to_csv(
                cur=cur, 
                sql_template=DEPARTURE_SQL, 
                fromdate=fromdate, 
                todate=todate,
                csv_path=os.path.join(target_directory, 'departure.csv')
            )
    except Exception as e:
        log.error(f'Could not copy from Transitlog DB: {e}')
    finally:
        if conn is not None:
            conn.close()

def main():
    try:
        fromdate = date.fromisoformat(sys.argv[1])
        todate = date.fromisoformat(sys.argv[2])
        transitlog_connstr = os.environ['TRANSITLOG_DB_CONNECTION_STRING']
    except IndexError:
        log.error('fromdate and todate required as command line arguments')
    except ValueError:
        log.error('fromdate and todate required in yyyy-mm-dd format')
    except KeyError:
        log.error('TRANSITLOG_DB_CONNECTION_STRING env variable value not found')
    copy_all_from_transitlog(
        transitlog_connstr=transitlog_connstr,
        fromdate=fromdate,
        todate=todate,
        target_directory='data/transitlog'
    )