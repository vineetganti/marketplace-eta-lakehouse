-- Source-of-truth OLTP schema for the marketplace. Debezium streams these tables.
-- All timestamps are timestamptz(3): the replayer writes SIMULATED event time into
-- updated_at (not wall clock), so event time in the data != processing time.
-- Doubles instead of numeric on purpose: keeps Debezium/Flink typing trivial.
-- (Exercise: switch fare_amount to numeric(10,2) and handle decimal.handling.mode.)

CREATE TABLE drivers (
    driver_id     BIGINT PRIMARY KEY,
    vehicle_type  TEXT             NOT NULL,   -- sedan | suv | bike | ev
    city          TEXT             NOT NULL,
    rating        DOUBLE PRECISION NOT NULL,
    status        TEXT             NOT NULL,   -- active | on_break | suspended
    updated_at    TIMESTAMPTZ(3)   NOT NULL
);

CREATE TABLE merchants (
    merchant_id    BIGINT PRIMARY KEY,
    category       TEXT             NOT NULL,  -- grocery | food | pharmacy | electronics
    tier           TEXT             NOT NULL,  -- bronze | silver | gold
    city           TEXT             NOT NULL,
    lat            DOUBLE PRECISION NOT NULL,
    lon            DOUBLE PRECISION NOT NULL,
    commission_pct DOUBLE PRECISION NOT NULL,
    updated_at     TIMESTAMPTZ(3)   NOT NULL
);

CREATE TABLE trips (
    trip_id        BIGINT PRIMARY KEY,
    rider_id       BIGINT           NOT NULL,
    driver_id      BIGINT,                      -- NULL until accepted
    merchant_id    BIGINT,                      -- set for service_type = 'delivery'
    service_type   TEXT             NOT NULL,   -- ride | delivery
    status         TEXT             NOT NULL,   -- requested | accepted | in_progress | completed | cancelled
    pickup_lat     DOUBLE PRECISION NOT NULL,
    pickup_lon     DOUBLE PRECISION NOT NULL,
    dropoff_lat    DOUBLE PRECISION NOT NULL,
    dropoff_lon    DOUBLE PRECISION NOT NULL,
    requested_at   TIMESTAMPTZ(3)   NOT NULL,   -- immutable; used as Iceberg partition source
    accepted_at    TIMESTAMPTZ(3),
    picked_up_at   TIMESTAMPTZ(3),
    completed_at   TIMESTAMPTZ(3),
    quoted_eta_sec INT              NOT NULL,   -- ETA quoted to the rider at request time (ride duration)
    fare_amount    DOUBLE PRECISION,            -- known at completion
    distance_km    DOUBLE PRECISION NOT NULL,
    updated_at     TIMESTAMPTZ(3)   NOT NULL
);

-- Full before-images: required to see old values on UPDATE/DELETE in the CDC stream.
ALTER TABLE drivers   REPLICA IDENTITY FULL;
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE trips     REPLICA IDENTITY FULL;

-- Debezium attaches to this publication (publication.autocreate.mode=disabled).
CREATE PUBLICATION lakehouse_pub FOR TABLE drivers, merchants, trips;
