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
  modified_at           timestamptz,
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
CREATE TRIGGER set_moddatetime    
  BEFORE INSERT OR UPDATE ON hfp.vehicle
  FOR EACH ROW
  EXECUTE PROCEDURE moddatetime(modified_at);


-- Journey model.
CREATE TABLE hfp.observed_journey (
  journey_id            uuid      PRIMARY KEY,
  route_id              text,
  direction_id          smallint,
  oday                  date,
  start                 interval,
  planned_operator_id   smallint,
  modified_at           timestamptz
);

COMMENT ON TABLE hfp.planned_journey IS
'Planned service operation through network path and pattern of stops.
Implicitly read from HFP (route_id, direction_id, oday, start, oper).';
COMMENT ON COLUMN hfp.planned_journey.journey_id IS
'Surrogate key for unique journeys. Generated as MD5 uuid from other columns separated with _.';
COMMENT ON COLUMN hfp.planned_journey.route_id IS
'Route identifier originating from Jore. `route_id` in HFP topic.';
COMMENT ON COLUMN hfp.planned_journey.direction_id IS
'Direction identifier originating from Jore: 1 or 2. `direction_id` in HFP topic.';
COMMENT ON COLUMN hfp.planned_journey.oday IS
'Operating date originating from Jore. `oday` in HFP payload.';
COMMENT ON COLUMN hfp.planned_journey.start IS
'Start time on the operating date, HH:MM:SS. `start` in HFP payload.
N.B. HFP uses 24h clock which can break journeys originally planned beyond >24:00:00.
Interval type is used for future support of such start times.';
COMMENT ON COLUMN hfp.planned_journey.planned_operator_id IS
'Id of the operator the journey was assigned to. `oper` in HFP payload.';

CREATE TRIGGER set_moddatetime    
  BEFORE INSERT OR UPDATE ON hfp.planned_journey
  FOR EACH ROW
  EXECUTE PROCEDURE moddatetime(modified_at);

CREATE FUNCTION hfp.tg_set_journey_id()
RETURNS trigger
AS $$
BEGIN
  -- E.g. ('1015', 2, '2020-04-04', '12:03:00', 19)
  -- is converted to '1015_2_2020-04-04_12:03:00_19'
  -- and then to md5 uuid '914e114a-86db-021d-d922-35d080f73387'.
  -- NULLs are skipped:
  -- ('1015', 2, '2020-04-04', NULL, 19)
  -- -> '1015_2_2020-04-04_19'
  -- -> '106d1bd4-260f-5a97-4618-04f823a3235f'.
  NEW.journey_id := md5(concat_ws('_',
    NEW.route_id, NEW.direction_id, NEW.operating_date, 
    NEW.start_time, NEW.journey_operator_id
  ))::uuid;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION hfp.tg_set_journey_id() IS
'Forces journey_id uuid value deterministically based on the other values.';

CREATE TRIGGER set_journey_id
  BEFORE INSERT OR UPDATE ON hfp.planned_journey
  FOR EACH ROW
  EXECUTE PROCEDURE hfp.tg_set_journey_id();


-- HFP observation model.
-- Some notes:
-- - For signed-off vehicles, journey_id should be NULL.
-- - Since we can assume that every event is also a VP vehicle position event,
--   this information need not be stored explicitly.
--   special_events shall only be populated if non-VP events were available
--   for that vehicle and second.
CREATE TABLE hfp.hfp_point (
  event_timestamp   timestamptz NOT NULL,
  vehicle_id        integer     NOT NULL REFERENCES hfp.vehicle(vehicle_id),
  journey_id        uuid            NULL REFERENCES hfp.planned_journey(journey_id),
  special_events    public.event_type[],
  received_at       timestamptz,
  odo               integer,
  drst              boolean,
  loc               public.location_source,
  geom              geometry(POINT, 3067),

  PRIMARY KEY (event_timestamp, vehicle_id)
);
CREATE INDEX ON hfp.hfp_point USING GIN(special_events)
  WHERE special_events IS NOT NULL;
CREATE INDEX ON hfp.hfp_point USING GIST(geom);

COMMENT ON TABLE hfp.hfp_point IS
'State of a transit vehicle at a time instant, based on HFP.';
COMMENT ON COLUMN hfp.hfp_point.event_timestamp IS
'Absolute timestamp of the observation, at full second precision.';
COMMENT ON COLUMN hfp.hfp_point.vehicle_id IS
'Unique id of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.journey_id IS
'Unique id of the journey the vehicle was possibly signed on.';
COMMENT ON COLUMN hfp.hfp_point.special_events IS
'Possible non-VP events triggered by the vehicle during that second.';
COMMENT ON COLUMN hfp.hfp_point.received_at IS
'Absolute timestamp when the underlying VP observation was received by server.';
COMMENT ON COLUMN hfp.hfp_point.odo IS
'Odometer value of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.drst IS
'Door status of the vehicle. TRUE if any door is open, FALSE if all closed, NULL if unknown.';
COMMENT ON COLUMN hfp.hfp_point.loc IS
'Source of the vehicle position information. Ideally GPS.';
COMMENT ON COLUMN hfp.hfp_point.geom IS
'Vehicle position point in ETRS-TM35 coordinates.';