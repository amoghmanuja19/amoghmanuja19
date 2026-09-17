-- Example Snowflake serving-layer queries

CREATE OR REPLACE VIEW machine_event_5m AS
SELECT
    window_start,
    window_end,
    event_type,
    event_count,
    avg_value
FROM streaming_event_aggregates;

SELECT
    event_type,
    SUM(event_count) AS events,
    ROUND(AVG(avg_value), 2) AS avg_value
FROM machine_event_5m
WHERE window_start >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY event_type
ORDER BY events DESC;
