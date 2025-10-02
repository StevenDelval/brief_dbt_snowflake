WITH filtered_trips AS (
    SELECT
        pickup_hour,
        total_amount,
        trip_duration_min,
        avg_speed_mph
    FROM {{ ref("clean_trips") }}
)
SELECT
    pickup_hour,
    COUNT(*) AS nb_trajet,
    AVG(total_amount) AS avg_revenue,
    AVG(avg_speed_mph) AS avg_speed_mph
FROM filtered_trips
GROUP BY pickup_hour