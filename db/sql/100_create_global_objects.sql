-- Extensions and global objects

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS moddatetime;

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

