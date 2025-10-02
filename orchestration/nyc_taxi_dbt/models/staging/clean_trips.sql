WITH filter_data as (
    SELECT
        VENDORID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PULOCATIONID,
        DOLOCATIONID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        CBD_CONGESTION_FEE
    FROM {{ source('raw_yellow_tripdata', 'YELLOW_TRIPDATA') }}
    WHERE FARE_AMOUNT >= 0
    AND TOTAL_AMOUNT >= 0
    AND TRIP_DISTANCE BETWEEN 0.1 AND 100
    AND TPEP_PICKUP_DATETIME < TPEP_DROPOFF_DATETIME 
    AND PASSENGER_COUNT > 0
    AND PULOCATIONID IS NOT NULL
    AND DOLOCATIONID IS NOT NULL
)
SELECT
    *,
    -- Durée du trajet en minutes
    DATEDIFF(minute, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME) AS TRIP_DURATION_MIN,

    -- Dimensions temporelles (basées sur pickup_datetime)
    EXTRACT(hour FROM TPEP_PICKUP_DATETIME) AS pickup_hour,
    EXTRACT(day FROM TPEP_PICKUP_DATETIME)  AS pickup_day,
    EXTRACT(month FROM TPEP_PICKUP_DATETIME) AS pickup_month,
    EXTRACT(year FROM TPEP_PICKUP_DATETIME) AS pickup_year,

    -- Vitesse moyenne (miles par heure)
    CASE 
        WHEN DATEDIFF(minute, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME) > 0 
        THEN TRIP_DISTANCE / (DATEDIFF(minute, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME) / 60.0)
        ELSE NULL
    END AS AVG_SPEED_MPH,

    -- Pourcentage du pourboire (par rapport au fare_amount uniquement)
    CASE 
        WHEN FARE_AMOUNT > 0 
        THEN (TIP_AMOUNT / FARE_AMOUNT) * 100
        ELSE NULL
    END AS TIP_PERCENTAGE

FROM filter_data