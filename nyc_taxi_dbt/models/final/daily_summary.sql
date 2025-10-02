WITH filter_data_daly_summary AS(
    SELECT
        CAST(CONCAT(pickup_year, '-', pickup_month, '-', pickup_day) AS DATE) AS pickup_date,
        trip_distance,
        total_amount
    FROM {{ ref("clean_trips") }}
)
SELECT 
    pickup_date,
    COUNT(*) as nb_trajet,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(total_amount) AS total_revenue
FROM filter_data_daly_summary
GROUP BY pickup_date