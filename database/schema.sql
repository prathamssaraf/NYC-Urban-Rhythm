-- Create PostGIS extension if not exists
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create neighborhoods table
CREATE TABLE neighborhoods (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    borough VARCHAR(50) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326)
);

-- Create 311 calls table
CREATE TABLE nyc_311_calls (
    id SERIAL PRIMARY KEY,
    created_date TIMESTAMP NOT NULL,
    complaint_type VARCHAR(255) NOT NULL,
    descriptor VARCHAR(255),
    incident_zip VARCHAR(10),
    geometry GEOMETRY(POINT, 4326),
    neighborhood_id INTEGER REFERENCES neighborhoods(id)
);

-- Create MTA turnstile table
CREATE TABLE mta_turnstile (
    id SERIAL PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    entries INTEGER NOT NULL,
    exits INTEGER NOT NULL,
    geometry GEOMETRY(POINT, 4326),
    neighborhood_id INTEGER REFERENCES neighborhoods(id)
);

-- Create TLC trips table
CREATE TABLE tlc_trips (
    id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pickup_location GEOMETRY(POINT, 4326),
    dropoff_location GEOMETRY(POINT, 4326),
    pickup_neighborhood_id INTEGER REFERENCES neighborhoods(id),
    dropoff_neighborhood_id INTEGER REFERENCES neighborhoods(id)
);

-- Create weather table
CREATE TABLE weather (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    temperature FLOAT,
    precipitation FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    weather_condition VARCHAR(50)
);

-- Create events table
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP NOT NULL,
    category VARCHAR(100),
    location GEOMETRY(POINT, 4326),
    neighborhood_id INTEGER REFERENCES neighborhoods(id)
);

-- Create spatial indexes
CREATE INDEX nyc_311_calls_geom_idx ON nyc_311_calls USING GIST(geometry);
CREATE INDEX mta_turnstile_geom_idx ON mta_turnstile USING GIST(geometry);
CREATE INDEX tlc_trips_pickup_geom_idx ON tlc_trips USING GIST(pickup_location);
CREATE INDEX tlc_trips_dropoff_geom_idx ON tlc_trips USING GIST(dropoff_location);
CREATE INDEX events_geom_idx ON events USING GIST(location);
CREATE INDEX neighborhoods_geom_idx ON neighborhoods USING GIST(geometry);

-- Create temporal indexes
CREATE INDEX nyc_311_calls_date_idx ON nyc_311_calls(created_date);
CREATE INDEX mta_turnstile_datetime_idx ON mta_turnstile(datetime);
CREATE INDEX tlc_trips_pickup_datetime_idx ON tlc_trips(pickup_datetime);
CREATE INDEX tlc_trips_dropoff_datetime_idx ON tlc_trips(dropoff_datetime);
CREATE INDEX weather_datetime_idx ON weather(datetime);
CREATE INDEX events_start_datetime_idx ON events(start_datetime);
CREATE INDEX events_end_datetime_idx ON events(end_datetime);


-- Add this to your database/schema.sql file

-- Table to track data pipeline task status
CREATE TABLE IF NOT EXISTS task_status (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    error TEXT,
    record_count INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster status lookups
CREATE INDEX IF NOT EXISTS task_status_source_idx ON task_status(source);
CREATE INDEX IF NOT EXISTS task_status_status_idx ON task_status(status);
CREATE INDEX IF NOT EXISTS task_status_updated_at_idx ON task_status(updated_at);

-- Add neighborhood_id field to data tables if not already present
ALTER TABLE nyc_311_calls ADD COLUMN IF NOT EXISTS neighborhood_id INTEGER REFERENCES neighborhoods(id);
ALTER TABLE mta_turnstile ADD COLUMN IF NOT EXISTS neighborhood_id INTEGER REFERENCES neighborhoods(id);
ALTER TABLE tlc_trips ADD COLUMN IF NOT EXISTS pickup_neighborhood_id INTEGER REFERENCES neighborhoods(id);
ALTER TABLE tlc_trips ADD COLUMN IF NOT EXISTS dropoff_neighborhood_id INTEGER REFERENCES neighborhoods(id);
ALTER TABLE events ADD COLUMN IF NOT EXISTS neighborhood_id INTEGER REFERENCES neighborhoods(id);

-- Update spatial indexes
CREATE INDEX IF NOT EXISTS nyc_311_calls_geom_idx ON nyc_311_calls USING GIST (geometry);
CREATE INDEX IF NOT EXISTS mta_turnstile_geom_idx ON mta_turnstile USING GIST (geometry);
CREATE INDEX IF NOT EXISTS tlc_trips_pickup_geom_idx ON tlc_trips USING GIST (pickup_location);
CREATE INDEX IF NOT EXISTS tlc_trips_dropoff_geom_idx ON tlc_trips USING GIST (dropoff_location);
CREATE INDEX IF NOT EXISTS events_geom_idx ON events USING GIST (geometry);

-- Update temporal indexes
CREATE INDEX IF NOT EXISTS nyc_311_calls_created_date_idx ON nyc_311_calls(created_date);
CREATE INDEX IF NOT EXISTS mta_turnstile_datetime_idx ON mta_turnstile(datetime);
CREATE INDEX IF NOT EXISTS tlc_trips_pickup_datetime_idx ON tlc_trips(pickup_datetime);
CREATE INDEX IF NOT EXISTS tlc_trips_dropoff_datetime_idx ON tlc_trips(dropoff_datetime);
CREATE INDEX IF NOT EXISTS weather_datetime_idx ON weather(datetime);
CREATE INDEX IF NOT EXISTS events_start_datetime_idx ON events(start_datetime);