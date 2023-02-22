--
-- Jore / Digitransit stops
--
-- Note that only one temporal snapshot of stops can be stored at a time,
-- i.e. no multiple versions of one stop_id.
--

CREATE SCHEMA jore;
COMMENT ON SCHEMA jore IS 'Data imported from Jore.';


CREATE TABLE jore.jore_stop (
  stop_id             integer                 PRIMARY KEY,
  stop_code           text,
  stop_name           text,
  parent_station      integer,
  stop_mode           text,
  route_dirs_via_stop text[],
  date_imported       date                    DEFAULT CURRENT_DATE,
  long                double precision,
  lat                 double precision,
  geom                geometry(POINT, 3067)   GENERATED ALWAYS AS (ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3067)) STORED
);
CREATE INDEX ON jore.jore_stop USING GIST (geom);


COMMENT ON TABLE jore.jore_stop IS
'Current HSL stops from Jore/Digitransit. See Digitransit for the first columns.';
COMMENT ON COLUMN jore.jore_stop.route_dirs_via_stop IS
'Route-direction combinations using the stop, according to Digitransit.';
COMMENT ON COLUMN jore.jore_stop.date_imported IS
'Date on which the stop was imported from Digitransit.';
COMMENT ON COLUMN jore.jore_stop.long IS
'WGS84 longitude (from Digitransit).';
COMMENT ON COLUMN jore.jore_stop.lat IS
'WGS84 latitude (from Digitransit).';
COMMENT ON COLUMN jore.jore_stop.geom IS
'Stop POINT geometry in ETRS-TM35 coordinate system, generated from long & lat.';

--
-- Jore / Digitransit stations
--
-- These are modeled just like stops but their purpose is a bit different:
-- We may want to report stop results grouped by stations / terminals,
-- with station attributes, such as name, attached.
-- A station is never directly referred to as a stop by an HFP observation.
--

CREATE TABLE jore.jore_station (
  LIKE jore.jore_stop INCLUDING ALL
);
COMMENT ON TABLE jore.jore_station IS
'Current HSL stations that may have stops belonging to them.';
