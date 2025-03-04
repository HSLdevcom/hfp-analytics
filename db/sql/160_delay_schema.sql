CREATE SCHEMA delay;
COMMENT ON SCHEMA delay IS 'Delay analysis data';


CREATE TABLE delay.preprocess_clusters (
    route_id  text NOT NULL,
    mode      text,
    from_date timestamp NOT NULL,
    to_date   timestamp NOT NULL,
    zst       bytea,
    PRIMARY KEY (route_id, from_date, to_date)
);

CREATE TABLE delay.preprocess_departures (
    route_id  text NOT NULL,
    mode      text,
    from_date timestamp NOT NULL,
    to_date   timestamp NOT NULL,
    zst       bytea,
    PRIMARY KEY (route_id, from_date, to_date)
);

CREATE TABLE delay.failure_debug_data (
    route_id  text NOT NULL,
    from_date timestamp NOT NULL,
    to_date   timestamp NOT NULL,
    zst       bytea,
    PRIMARY KEY (route_id, from_date, to_date)
);

CREATE TABLE delay.route_clusters (
    route_id  text NOT NULL,
    from_date timestamp NOT NULL,
    to_date   timestamp NOT NULL,
    zst       bytea,
    PRIMARY KEY (route_id, from_date, to_date)
);

CREATE TABLE delay.mode_clusters (
    route_id  text NOT NULL,
    from_date timestamp NOT NULL,
    to_date   timestamp NOT NULL,
    zst       bytea,
    PRIMARY KEY (route_id, from_date, to_date)
);