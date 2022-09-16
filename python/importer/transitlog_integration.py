"""Import data from Transitlog DB to HFP Analytics DB."""

import os
import psycopg2
from datetime import date

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

def import_all(
    transitlog_conn: psycopg2.connection, 
    analytics_conn: psycopg2.connection, 
    fromdate: date, 
    todate: date
) -> None:
    """Import all required Jore data concerning the given date range
    from Transitlog DB to HFP Analytics DB."""
    pass