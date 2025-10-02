WITH zone_agg AS (
    SELECT
        PULOCATIONID,
        
        -- Volume de trajets
        COUNT(*) AS trip_volume,

        -- Revenus moyens (basé sur TOTAL_AMOUNT)
        AVG(TOTAL_AMOUNT) AS avg_revenue,

        -- Popularité (proportion des trajets depuis cette zone)
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS popularity_pct
    FROM {{ ref("clean_trips") }}
    GROUP BY PULOCATIONID

)
SELECT *
FROM zone_agg