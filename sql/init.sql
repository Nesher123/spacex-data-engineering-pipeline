CREATE TABLE
    IF NOT EXISTS raw_launches (
        launch_id VARCHAR PRIMARY KEY,
        mission_name VARCHAR,
        date_utc TIMESTAMPTZ NOT NULL,
        success BOOLEAN,
        payload_ids JSONB,
        launchpad_id VARCHAR,
        static_fire_date_utc TIMESTAMPTZ,
        ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE
    IF NOT EXISTS ingestion_state (
        id SERIAL PRIMARY KEY,
        last_fetched_date TIMESTAMPTZ,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );