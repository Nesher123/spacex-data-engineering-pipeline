-- Create raw launches table (for append-only ingestions)
CREATE TABLE
    IF NOT EXISTS raw_launches (
        launch_id VARCHAR PRIMARY KEY,
        mission_name VARCHAR,
        date_utc TIMESTAMPTZ NOT NULL,
        success BOOLEAN,
        payload_ids JSONB,
        total_payload_mass_kg DECIMAL(10, 2),
        launchpad_id VARCHAR,
        static_fire_date_utc TIMESTAMPTZ,
        ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

-- Create ingestion state table (for tracking ingestion progress)
CREATE TABLE
    IF NOT EXISTS ingestion_state (
        id SERIAL PRIMARY KEY,
        last_fetched_date TIMESTAMPTZ,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

-- Time-series aggregation table for trend analysis
CREATE TABLE
    IF NOT EXISTS launch_aggregations (
        id SERIAL PRIMARY KEY,
        total_launches BIGINT NOT NULL DEFAULT 0,
        total_successful_launches BIGINT NOT NULL DEFAULT 0,
        total_failed_launches BIGINT NOT NULL DEFAULT 0,
        success_rate DECIMAL(5, 2),
        earliest_launch_date TIMESTAMPTZ,
        latest_launch_date TIMESTAMPTZ,
        total_launch_sites BIGINT DEFAULT 0,
        average_payload_mass_kg DECIMAL(10, 2),
        average_delay_hours DECIMAL(10, 2),
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        last_processed_launch_date TIMESTAMPTZ,
        -- Time-series fields for trend analysis
        snapshot_type VARCHAR(50) DEFAULT 'incremental', -- 'initial', 'incremental', 'manual'
        launches_added_in_batch BIGINT DEFAULT 0,
        pipeline_run_id VARCHAR(100) -- Optional: link to specific pipeline runs
    );

-- Index for efficient time-series queries
CREATE INDEX IF NOT EXISTS idx_launch_aggregations_updated_at ON launch_aggregations (updated_at DESC);

-- Index for querying by snapshot type
CREATE INDEX IF NOT EXISTS idx_launch_aggregations_snapshot_type ON launch_aggregations (snapshot_type, updated_at DESC);