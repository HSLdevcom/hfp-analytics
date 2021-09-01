-- Test stop_id guessing with different distance limits.

BEGIN;

UPDATE observation SET stop_id = NULL, stop_id_guessed = NULL WHERE stop_id_guessed;

DO $$
DECLARE
  dist float8;
  i integer;
  res integer;
BEGIN
  FOR i IN 10..200 BY 10 LOOP
    dist := i::float8;
    SELECT INTO res * FROM guess_missing_stop_ids(dist);
    RAISE NOTICE '% m: % updated', dist, res;
    UPDATE observation SET stop_id = NULL, stop_id_guessed = NULL WHERE stop_id_guessed;
  END LOOP;
END;
$$;

ROLLBACK;
