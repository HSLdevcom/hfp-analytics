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
CREATE TRIGGER set_moddatetime    
  BEFORE UPDATE ON hfp.vehicle
  FOR EACH ROW
  EXECUTE PROCEDURE moddatetime(modified_at);


-- Journey model.
CREATE TABLE hfp.observed_journey (
  journey_id            uuid          PRIMARY KEY,
  route_id              text,
  direction_id          smallint,
  oday                  date,
  start                 interval,
  planned_operator_id   smallint,
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
COMMENT ON COLUMN hfp.observed_journey.planned_operator_id IS
'Id of the operator the journey was assigned to. `oper` in HFP payload.';

CREATE TRIGGER set_moddatetime    
  BEFORE UPDATE ON hfp.observed_journey
  FOR EACH ROW
  EXECUTE PROCEDURE moddatetime(modified_at);


-- HFP observation model.
-- Some notes:
-- - journey_id can be NULL, meaning the vehicle was not signed in to any journey.
CREATE TABLE hfp.hfp_point (
  event_timestamp   timestamptz NOT NULL,
  vehicle_id        integer     NOT NULL REFERENCES hfp.vehicle(vehicle_id),
  journey_id        uuid            NULL REFERENCES hfp.observed_journey(journey_id),
  hfp_events        public.event_type[],
  received_at       timestamptz,
  odo               real,
  drst              boolean,
  loc               public.location_source,
  stop              integer,
  geom              geometry(POINT, 3067),

  PRIMARY KEY (event_timestamp, vehicle_id)
);
CREATE INDEX ON hfp.hfp_point USING GIN(hfp_events);
CREATE INDEX ON hfp.hfp_point USING GIST(geom);

COMMENT ON TABLE hfp.hfp_point IS
'State of a transit vehicle at a time instant, based on HFP.';
COMMENT ON COLUMN hfp.hfp_point.event_timestamp IS
'Absolute timestamp of the observation, at full second precision.';
COMMENT ON COLUMN hfp.hfp_point.vehicle_id IS
'Unique id of the vehicle.';
COMMENT ON COLUMN hfp.hfp_point.journey_id IS
'Unique id of the journey the vehicle was possibly signed on.';
COMMENT ON COLUMN hfp.hfp_point.hfp_events IS
'Possible non-VP events triggered by the vehicle during that second.';
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