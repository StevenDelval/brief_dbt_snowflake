DESCRIBE TABLE NYC_TAXI_DB.RAW.YELLOW_TRIPDATA;

SELECT
    COUNT(*) AS total_rows,
    COUNT(vendorid) AS vendorid_not_null,
    COUNT(tpep_pickup_datetime) AS pickup_not_null,
    COUNT(tpep_dropoff_datetime) AS dropoff_not_null,
    COUNT(passenger_count) AS passenger_not_null,
    COUNT(trip_distance) AS distance_not_null,
    COUNT(ratecodeid) AS ratecode_not_null,
    COUNT(store_and_fwd_flag) AS store_not_null,
    COUNT(pulocationid) AS pulocation_not_null,
    COUNT(dolocationid) AS dolocation_not_null,
    COUNT(payment_type) AS payment_not_null,
    COUNT(fare_amount) AS fare_not_null,
    COUNT(extra) AS extra_not_null,
    COUNT(mta_tax) AS mta_tax_not_null,
    COUNT(tip_amount) AS tip_not_null,
    COUNT(tolls_amount) AS tolls_not_null,
    COUNT(improvement_surcharge) AS surcharge_not_null,
    COUNT(total_amount) AS total_not_null,
    COUNT(congestion_surcharge) AS congestion_not_null,
    COUNT(airport_fee) AS airport_fee_not_null,
    COUNT(cbd_congestion_fee) AS cbd_fee_not_null,
    COUNT(file_name) AS file_name_not_null
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA;

SELECT
    -- Passenger count
    MIN(passenger_count) AS min_passenger,
    MAX(passenger_count) AS max_passenger,
    AVG(passenger_count) AS avg_passenger,
    APPROX_PERCENTILE(passenger_count, 0.25) AS q1_passenger,
    APPROX_PERCENTILE(passenger_count, 0.50) AS median_passenger,
    APPROX_PERCENTILE(passenger_count, 0.75) AS q3_passenger,

    -- Trip distance
    MIN(trip_distance) AS min_distance,
    MAX(trip_distance) AS max_distance,
    AVG(trip_distance) AS avg_distance,
    APPROX_PERCENTILE(trip_distance, 0.25) AS q1_distance,
    APPROX_PERCENTILE(trip_distance, 0.50) AS median_distance,
    APPROX_PERCENTILE(trip_distance, 0.75) AS q3_distance,

    -- Fare amount
    MIN(fare_amount) AS min_fare,
    MAX(fare_amount) AS max_fare,
    AVG(fare_amount) AS avg_fare,
    APPROX_PERCENTILE(fare_amount, 0.25) AS q1_fare,
    APPROX_PERCENTILE(fare_amount, 0.50) AS median_fare,
    APPROX_PERCENTILE(fare_amount, 0.75) AS q3_fare,

    -- Tolls amount
    MIN(tolls_amount) AS min_tolls,
    MAX(tolls_amount) AS max_tolls,
    AVG(tolls_amount) AS avg_tolls,
    APPROX_PERCENTILE(tolls_amount, 0.25) AS q1_tolls,
    APPROX_PERCENTILE(tolls_amount, 0.50) AS median_tolls,
    APPROX_PERCENTILE(tolls_amount, 0.75) AS q3_tolls,

    -- Total amount
    MIN(total_amount) AS min_total,
    MAX(total_amount) AS max_total,
    AVG(total_amount) AS avg_total,
    APPROX_PERCENTILE(total_amount, 0.25) AS q1_total,
    APPROX_PERCENTILE(total_amount, 0.50) AS median_total,
    APPROX_PERCENTILE(total_amount, 0.75) AS q3_total

FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA;

SELECT 
    COUNT(DISTINCT vendorid) AS vendor_count,
    COUNT(DISTINCT ratecodeid) AS ratecode_count,
    COUNT(DISTINCT pulocationid) AS pickup_count,
    COUNT(DISTINCT dolocationid) AS dropoff_count,
    COUNT(DISTINCT payment_type) AS payment_count
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA;

WITH nb_row as (
SELECT COUNT(*) AS total_rows
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA
)
SELECT
    COUNT(*) AS invalid_trips,
    COUNT(*) * 100.0 / (SELECT total_rows FROM nb_row) AS percent_invalid
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA
WHERE tpep_pickup_datetime >= tpep_dropoff_datetime;


SELECT
    COUNT(*) AS invalid_trips,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA) AS percent_invalid
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA
WHERE fare_amount < 0;

SELECT
    COUNT(*) AS invalid_trips,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA) AS percent_invalid
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA
WHERE total_amount < 0;

SELECT
    COUNT(*) AS invalid_trips,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA) AS percent_invalid
FROM NYC_TAXI_DB.RAW.YELLOW_TRIPDATA
WHERE trip_distance > 1000;
