CREATE SCHEMA logs;
-- Important: tables here should be named as logs.<function name>_log

-- Log levels
-- Not modelled as enum to allow more convenient queries;
-- enum values are handy in big data tables
-- but logs.api_log is not such one.
CREATE TABLE logs.log_level (
  log_level        text          PRIMARY KEY
);
COMMENT ON TABLE logs.log_level IS
'Log levels allowed for log tables log_level values.';
INSERT INTO logs.log_level (log_level)
VALUES ('info'), ('warning'), ('error'), ('debug');

--
CREATE TABLE logs.importer_log (
  id bigserial PRIMARY KEY,
  log_timestamp timestamptz DEFAULT now(),
  log_level text NULL REFERENCES logs.log_level(log_level),
  log_text text
);
COMMENT ON TABLE logs.importer_log IS
'Logs for importer with different log_levels: info, warning, error, debug.';

CREATE TABLE logs.api_log (
  id bigserial PRIMARY KEY,
  log_timestamp timestamptz DEFAULT now(),
  log_level text NULL REFERENCES logs.log_level(log_level),
  log_text text
);
COMMENT ON TABLE logs.api_log IS
'Logs for api with different log_levels: info, warning, error, debug.';