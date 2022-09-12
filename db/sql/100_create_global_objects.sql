-- Extensions and global objects

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS moddatetime;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE FUNCTION array_distinct(anyarray) 
RETURNS anyarray 
AS $f$
  SELECT array_agg(DISTINCT x) FROM unnest($1) t(x);
$f$ LANGUAGE SQL IMMUTABLE;
COMMENT ON FUNCTION array_distinct IS
'Eliminates any duplicates from an array. E.g.
 > SELECT array_distinct_notnull(array[1, 2, 2]);
  array_distinct
═════════════════
  {1,2}';

CREATE OR REPLACE FUNCTION is_lock_enabled(lock_id int)
RETURNS boolean
VOLATILE
LANGUAGE SQL
AS $func$
  SELECT EXISTS (
    SELECT mode, classid, objid FROM pg_locks
    WHERE locktype = 'advisory' AND objid = lock_id
  )
$func$;
