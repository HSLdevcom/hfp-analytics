CREATE SCHEMA hfp;
COMMENT ON SCHEMA hfp IS
'Models transit vehicle state, position and event data (High-Frequency Positioning).';


-- Transport modes.
-- Not modelled as enum to allow more convenient queries;
-- enum values are handy in big data tables
-- but hfp.vehicle is not such one.
CREATE TABLE hfp.transport_mode (
  transport_mode        text          PRIMARY KEY
);
COMMENT ON TABLE hfp.transport_mode IS
'Dimension table for allowed transport_mode values.';
INSERT INTO hfp.transport_mode (transport_mode)
VALUES ('bus'), ('tram'), ('metro'), ('train'), ('ferry'), ('ubus');

-- Vehicle model.
CREATE TABLE hfp.vehicle (
  vehicle_id            integer       PRIMARY KEY,
  vehicle_operator_id   smallint      NOT NULL,
  vehicle_number        smallint      NOT NULL,
  transport_mode        text              NULL REFERENCES hfp.transport_mode(transport_mode),
  modified_at           timestamptz   DEFAULT now(),
  CONSTRAINT vehicle_id_format CHECK (
    vehicle_id = (100000*vehicle_operator_id + vehicle_number)
  )
);
COMMENT ON TABLE hfp.vehicle IS
'Transit vehicle, unique by owner operator and vehicle identifier.';
COMMENT ON COLUMN hfp.vehicle.vehicle_id IS
'Surrogate key for unique vehicles, deterministically generated from
vehicle number (5 rightmost digits) and operator id (leftmost digits).';
COMMENT ON COLUMN hfp.vehicle.vehicle_operator_id IS
'Id of the operator who owns the vehicle. `operator_id` in HFP topic.';
COMMENT ON COLUMN hfp.vehicle.vehicle_number IS
'Vehicle number, unique within operator. `vehicle_number` in HFP topic.';
COMMENT ON COLUMN hfp.vehicle.transport_mode IS
'Mode of the vehicle. `transport_mode` in HFP topic.';
COMMENT ON COLUMN hfp.vehicle.modified_at IS
'When the vehicle row was added or last modified.';
CREATE TRIGGER set_modified_at    
  BEFORE UPDATE ON hfp.vehicle
  FOR EACH ROW
  EXECUTE PROCEDURE set_modified_at();


-- Journey model.
CREATE TABLE hfp.observed_journey (
  journey_id            uuid          PRIMARY KEY,
  route_id              text,
  direction_id          smallint,
  oday                  date,
  start                 interval,
  observed_operator_id  smallint,
  modified_at           timestamptz   DEFAULT now()
);

COMMENT ON TABLE hfp.observed_journey IS
'Planned service operation through network path and pattern of stops.
Implicitly read from HFP (route_id, direction_id, oday, start, oper).';
COMMENT ON COLUMN hfp.observed_journey.journey_id IS
'Surrogate key for unique journeys. Generated as MD5 uuid from other columns separated with _.';
COMMENT ON COLUMN hfp.observed_journey.route_id IS
'Route identifier originating from Jore. `route_id` in HFP topic.';
COMMENT ON COLUMN hfp.observed_journey.direction_id IS
'Direction identifier originating from Jore: 1 or 2. `direction_id` in HFP topic.';
COMMENT ON COLUMN hfp.observed_journey.oday IS
'Operating date originating from Jore. `oday` in HFP payload.';
COMMENT ON COLUMN hfp.observed_journey.start IS
'Start time on the operating date, HH:MM:SS. `start` in HFP payload.
N.B. HFP uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN hfp.observed_journey.observed_operator_id IS
'Id of the operator the journey was assigned to. `oper` in HFP payload.';

CREATE TRIGGER set_modified_at    
  BEFORE UPDATE ON hfp.observed_journey
  FOR EACH ROW
  EXECUTE PROCEDURE set_modified_at();


-- HFP observation model.
-- Some notes:
-- - journey_id can be NULL, meaning the vehicle was not signed in to any journey.
CREATE TABLE hfp.hfp_point (
  point_timestamp   timestamptz NOT NULL,
  vehicle_id        integer     NOT NULL REFERENCES hfp.vehicle(vehicle_id),
  journey_id        uuid            NULL REFERENCES hfp.observed_journey(journey_id) ON DELETE CASCADE,
  hfp_events        text[],
  received_at       timestamptz,
  odo               real,
  drst              boolean,
  loc               text,
  stop              integer,
  geom              geometry(POINT, 3067),

  PRIMARY KEY (point_timestamp, vehicle_id)
);
CREATE INDEX ON hfp.hfp_point USING GIN(hfp_events);
CREATE INDEX ON hfp.hfp_point USING GIST(geom);

COMMENT ON TABLE hfp.hfp_point IS
'State of a transit vehicle at a time instant, based on HFP.';
COMMENT ON COLUMN hfp.hfp_point.point_timestamp IS
'Absolute timestamp of the observation, at full second precision.';
COMMENT ON COLUMN hfp.hfp_point.vehicle_id IS
'Unique id of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.journey_id IS
'Unique id of the journey the vehicle was possibly signed on.';
COMMENT ON COLUMN hfp.hfp_point.hfp_events IS
'HFP events triggered by the vehicle during that second.';
COMMENT ON COLUMN hfp.hfp_point.received_at IS
'Absolute timestamp when the underlying VP observation was received by server.';
COMMENT ON COLUMN hfp.hfp_point.odo IS
'Odometer value of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.drst IS
'Door status of the vehicle. TRUE if any door is open, FALSE if all closed, NULL if unknown.';
COMMENT ON COLUMN hfp.hfp_point.loc IS
'Source of the vehicle position information. Ideally GPS.';
COMMENT ON COLUMN hfp.hfp_point.stop IS
'Id of the stop that the HFP point was related to.';
COMMENT ON COLUMN hfp.hfp_point.geom IS
'Vehicle position point in ETRS-TM35 coordinates.';


CREATE VIEW hfp.view_as_original_hfp_event AS (
  SELECT
    hp.point_timestamp                AS tst,
    unnest(hp.hfp_events)             AS event_type,
    hp.received_at,
    ve.vehicle_operator_id,
    ve.vehicle_number,
    ve.transport_mode,
    oj.route_id,
    oj.direction_id,
    oj.oday,
    oj.start,
    oj.observed_operator_id,
    hp.odo,
    hp.drst,
    hp.loc,
    hp.stop,
    ST_X(ST_Transform(hp.geom, 4326)) AS longitude,
    ST_Y(ST_Transform(hp.geom, 4326)) AS latitude
  FROM
    hfp.hfp_point                   AS hp
    INNER JOIN hfp.vehicle          AS ve
      ON (hp.vehicle_id = ve.vehicle_id)
    LEFT JOIN hfp.observed_journey  AS oj
      ON (hp.journey_id = oj.journey_id)
);
COMMENT ON VIEW hfp.view_as_original_hfp_event IS
'Exposes HFP points with event types decomposed into separate rows
and with vehicle and journey attributes, like in original HFP data.
Used primarily for data imports through INSTEAD OF INSERT trigger.';

CREATE FUNCTION hfp.tg_hfp_insertor()
RETURNS trigger
AS $$
DECLARE
  vehid integer;
  jrnid uuid;
BEGIN
  -- Use variables for these to avoid repeated calculations in further steps.
  vehid := 100000*NEW.vehicle_operator_id + NEW.vehicle_number;

  jrnid := md5(concat_ws('_',
    NEW.route_id, NEW.direction_id, NEW.oday, 
    NEW.start, NEW.observed_operator_id
  ))::uuid;

  -- Insert the vehicle row, do nothing if exists.
  INSERT INTO hfp.vehicle (vehicle_id, vehicle_operator_id, vehicle_number, transport_mode)
  VALUES (vehid, NEW.vehicle_operator_id, NEW.vehicle_number, NEW.transport_mode)
  ON CONFLICT ON CONSTRAINT vehicle_pkey DO NOTHING;

  -- Insert the journey row but only with non-null journey attributes, do nothing if exists.
  -- NOTE: Even if some journey attributes are null (e.g. route&dir available but the others not),
  --       we still want to catch them as a "journey" existing in HFP data
  --       so we're able to monitor whether such incomplete journeys occur.
  -- "(foo, bar, baz) IS NULL" returns TRUE only if foo, bar AND baz are all NULL.
  IF NOT ((NEW.route_id, NEW.direction_id, NEW.oday, NEW.start, NEW.observed_operator_id) IS NULL) THEN
    INSERT INTO hfp.observed_journey (journey_id, route_id, direction_id, oday, start, observed_operator_id)
    VALUES (jrnid, NEW.route_id, NEW.direction_id, NEW.oday, NEW.start, NEW.observed_operator_id)
    ON CONFLICT ON CONSTRAINT observed_journey_pkey DO NOTHING;
  END IF;

  -- Insert the hfp_point row, making the following transformations:
  -- - Timestamp, originally tst, is truncated to full second.
  -- - WGS84 long and lat are converted to ETRS-TM35 point geometry.
  -- For existing data, by (point_timestamp, vehicle_id):
  -- - Event type array is appended with the new event, though only if that type did not exist there.
  -- - Other fields are updated only if they were previously NULL (coalesce(old_value, new_candidate)).
  INSERT INTO hfp.hfp_point AS hp (
    point_timestamp, vehicle_id, journey_id, hfp_events, received_at, odo, drst, loc, stop, geom
  )
  VALUES (
    date_trunc('second', NEW.tst),
    vehid,
    jrnid,
    array[NEW.event_type],
    NEW.received_at,
    NEW.odo,
    NEW.drst,
    NEW.loc,
    NEW.stop,
    ST_Transform( ST_SetSRID( ST_MakePoint(NEW.longitude, NEW.latitude), 4326), 3067)
  )
  ON CONFLICT (point_timestamp, vehicle_id) DO UPDATE
  SET
    journey_id = coalesce(hp.journey_id, EXCLUDED.journey_id),
    hfp_events = array_distinct( array_cat(hp.hfp_events, EXCLUDED.hfp_events)),
    received_at = coalesce(hp.received_at, EXCLUDED.received_at),
    odo = coalesce(hp.odo, EXCLUDED.odo),
    drst = coalesce(hp.drst, EXCLUDED.drst),
    loc = coalesce(hp.loc, EXCLUDED.loc),
    stop = coalesce(hp.stop, EXCLUDED.stop),
    geom = coalesce(hp.geom, EXCLUDED.geom)
  ;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION hfp.tg_hfp_insertor() IS
'When data is imported against view_as_original_hfp_event view,
this function populates the actual data tables hfp_point, vehicle,
and journey, and merges different events of same second and vehicle
to one row. For already existing data, only missing values are filled.
Any row that is inserted first becomes the "root" data of that hfp_point
and related rows in other tables.';

CREATE TRIGGER insert_hfp_data_through_view
  INSTEAD OF INSERT ON hfp.view_as_original_hfp_event
  FOR EACH ROW 
  EXECUTE FUNCTION hfp.tg_hfp_insertor();