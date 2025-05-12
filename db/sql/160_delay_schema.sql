CREATE SCHEMA delay;
COMMENT ON SCHEMA delay IS 'Delay analysis data';


CREATE TABLE delay.preprocess_clusters (
    route_id  text NOT NULL,
    oday      DATE NOT NULL,
    mode      text,
    zst       bytea,
    PRIMARY KEY (route_id, oday)
);

CREATE TABLE delay.preprocess_departures (
    route_id  text NOT NULL,
    oday      DATE NOT NULL,
    mode      text,
    zst       bytea,
    PRIMARY KEY (route_id, oday)
);

CREATE TABLE delay.recluster_routes(
    route_id   text NOT NULL,
    from_oday  DATE NOT NULL,
    to_oday    DATE NOT NULL,
    mode       text,
    zst        bytea,
    status     text,
    createdAt  timestamptz NOT NULL DEFAULT now(),
    modifiedAt timestamptz NULL,
    PRIMARY KEY (route_id, from_oday, to_oday)
);

CREATE TABLE delay.recluster_modes (
    route_id  text NOT NULL,
    from_oday DATE NOT NULL,
    to_oday   DATE NOT NULL,
    mode      text,
    zst       bytea,
    PRIMARY KEY (route_id, from_oday, to_oday)
);

CREATE TABLE delay.failure_debug_data (
    route_id  text NOT NULL,
    oday      DATE NOT NULL,
    zst       bytea,
    PRIMARY KEY (route_id, oday)
);