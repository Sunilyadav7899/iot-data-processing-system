-- IoT Data Processing System Database Schema
-- Initialize database tables for temperature and window sensor data

-- Create database and user (if not exists via environment)
-- This script runs as part of PostgreSQL initialization

-- Temperature readings table (raw data)
CREATE TABLE IF NOT EXISTS temperature_readings (
    id SERIAL PRIMARY KEY,
    timestamp_epoch BIGINT NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    temperature_fahrenheit DOUBLE PRECISION NOT NULL,
    temperature_celsius DOUBLE PRECISION NOT NULL,
    message_type VARCHAR(50) DEFAULT 'temperature_reading',
    producer_timestamp TIMESTAMP,
    event_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_type VARCHAR(20) DEFAULT 'NORMAL',
    kafka_timestamp TIMESTAMP,
    message_key VARCHAR(100)
);

-- Create indexes for temperature readings
CREATE INDEX IF NOT EXISTS idx_temperature_readings_device_timestamp 
    ON temperature_readings(device_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_temperature_readings_timestamp 
    ON temperature_readings(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_temperature_readings_anomaly 
    ON temperature_readings(is_anomaly, anomaly_type);

-- Temperature aggregations - 1 minute windows
CREATE TABLE IF NOT EXISTS temperature_aggregations_1min (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    window_duration VARCHAR(20) DEFAULT '1_minute',
    avg_temp_celsius DOUBLE PRECISION,
    avg_temp_fahrenheit DOUBLE PRECISION,
    max_temp_celsius DOUBLE PRECISION,
    min_temp_celsius DOUBLE PRECISION,
    reading_count BIGINT,
    anomaly_count BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for 1-minute aggregations
CREATE INDEX IF NOT EXISTS idx_temp_agg_1min_device_window 
    ON temperature_aggregations_1min(device_id, window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_temp_agg_1min_window_start 
    ON temperature_aggregations_1min(window_start);

-- Temperature aggregations - 5 minute windows
CREATE TABLE IF NOT EXISTS temperature_aggregations_5min (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    window_duration VARCHAR(20) DEFAULT '5_minute',
    avg_temp_celsius DOUBLE PRECISION,
    avg_temp_fahrenheit DOUBLE PRECISION,
    max_temp_celsius DOUBLE PRECISION,
    min_temp_celsius DOUBLE PRECISION,
    reading_count BIGINT,
    anomaly_count BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for 5-minute aggregations
CREATE INDEX IF NOT EXISTS idx_temp_agg_5min_device_window 
    ON temperature_aggregations_5min(device_id, window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_temp_agg_5min_window_start 
    ON temperature_aggregations_5min(window_start);

-- Window events table (raw data)
CREATE TABLE IF NOT EXISTS window_events (
    id SERIAL PRIMARY KEY,
    object_code INTEGER NOT NULL,
    object_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(20) NOT NULL, -- 'opened' or 'closed'
    message_type VARCHAR(50) DEFAULT 'window_event',
    producer_timestamp TIMESTAMP,
    event_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    message_key VARCHAR(100)
);

-- Create indexes for window events
CREATE INDEX IF NOT EXISTS idx_window_events_object_timestamp 
    ON window_events(object_code, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_window_events_timestamp 
    ON window_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_window_events_type 
    ON window_events(event_type);

-- Window aggregations - hourly
CREATE TABLE IF NOT EXISTS window_aggregations_hourly (
    id SERIAL PRIMARY KEY,
    object_code INTEGER NOT NULL,
    object_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    aggregation_period VARCHAR(20) DEFAULT 'hourly',
    event_count BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for hourly window aggregations
CREATE INDEX IF NOT EXISTS idx_window_agg_hourly_object_window 
    ON window_aggregations_hourly(object_code, window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_window_agg_hourly_window_start 
    ON window_aggregations_hourly(window_start);

-- Window aggregations - daily
CREATE TABLE IF NOT EXISTS window_aggregations_daily (
    id SERIAL PRIMARY KEY,
    object_code INTEGER NOT NULL,
    object_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    aggregation_period VARCHAR(20) DEFAULT 'daily',
    event_count BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for daily window aggregations
CREATE INDEX IF NOT EXISTS idx_window_agg_daily_object_window 
    ON window_aggregations_daily(object_code, window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_window_agg_daily_window_start 
    ON window_aggregations_daily(window_start);

-- Window durations table (for tracking open/close cycles)
CREATE TABLE IF NOT EXISTS window_durations (
    id SERIAL PRIMARY KEY,
    object_code INTEGER NOT NULL,
    object_name VARCHAR(100) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    duration_seconds BIGINT NOT NULL,
    duration_minutes DOUBLE PRECISION NOT NULL,
    usage_pattern VARCHAR(20), -- 'BRIEF_OPENING', 'SHORT_OPENING', etc.
    is_unusual BOOLEAN DEFAULT FALSE,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for window durations
CREATE INDEX IF NOT EXISTS idx_window_durations_object_opened 
    ON window_durations(object_code, opened_at);
CREATE INDEX IF NOT EXISTS idx_window_durations_duration 
    ON window_durations(duration_minutes);
CREATE INDEX IF NOT EXISTS idx_window_durations_unusual 
    ON window_durations(is_unusual);

-- System monitoring table
CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_unit VARCHAR(20),
    tags JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for system metrics
CREATE INDEX IF NOT EXISTS idx_system_metrics_name_timestamp 
    ON system_metrics(metric_name, timestamp);

-- Create views for easier querying

-- Latest temperature readings per device
CREATE OR REPLACE VIEW latest_temperature_readings AS
SELECT DISTINCT ON (device_id)
    device_id,
    temperature_celsius,
    temperature_fahrenheit,
    event_timestamp,
    is_anomaly,
    anomaly_type
FROM temperature_readings
ORDER BY device_id, event_timestamp DESC;

-- Temperature statistics by device (last 24 hours)
CREATE OR REPLACE VIEW temperature_stats_24h AS
SELECT 
    device_id,
    COUNT(*) as reading_count,
    AVG(temperature_celsius) as avg_temp_celsius,
    MIN(temperature_celsius) as min_temp_celsius,
    MAX(temperature_celsius) as max_temp_celsius,
    STDDEV(temperature_celsius) as stddev_temp_celsius,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count
FROM temperature_readings
WHERE event_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY device_id;

-- Window activity summary (last 7 days)
CREATE OR REPLACE VIEW window_activity_7d AS
SELECT 
    object_code,
    object_name,
    COUNT(*) as total_events,
    COUNT(CASE WHEN event_type = 'opened' THEN 1 END) as open_events,
    COUNT(CASE WHEN event_type = 'closed' THEN 1 END) as close_events,
    DATE_TRUNC('day', event_timestamp) as activity_date
FROM window_events
WHERE event_timestamp > NOW() - INTERVAL '7 days'
GROUP BY object_code, object_name, DATE_TRUNC('day', event_timestamp)
ORDER BY activity_date DESC, object_code;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO iot_user;

-- Insert some initial system metrics
INSERT INTO system_metrics (metric_name, metric_value, metric_unit, tags) VALUES
    ('system_initialized', 1, 'boolean', '{"component": "database", "version": "1.0"}'),
    ('tables_created', 9, 'count', '{"type": "main_tables"}'),
    ('views_created', 3, 'count', '{"type": "monitoring_views"}');

-- Create a function to clean old data (optional)
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    -- Clean old temperature readings (keep aggregations)
    DELETE FROM temperature_readings 
    WHERE event_timestamp < NOW() - (days_to_keep || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Log the cleanup
    INSERT INTO system_metrics (metric_name, metric_value, metric_unit, tags)
    VALUES ('data_cleanup_temp_readings', deleted_count, 'rows_deleted', 
            json_build_object('days_to_keep', days_to_keep)::jsonb);
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMIT;