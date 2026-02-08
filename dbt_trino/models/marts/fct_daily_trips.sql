{{ config(
    materialized='table',
    schema='gold',
    properties={
      "format": "'PARQUET'"
    }
) }}

SELECT
    pickup_date,
    pickup_day_of_week,
    COUNT(*)                                            AS total_trips,
    SUM(passenger_count)                                AS total_passengers,
    ROUND(AVG(trip_distance), 2)                        AS avg_trip_distance,
    ROUND(AVG(trip_duration_seconds / 60.0), 2)         AS avg_trip_duration_minutes,
    ROUND(SUM(fare_amount), 2)                          AS total_fare,
    ROUND(SUM(tip_amount), 2)                           AS total_tips,
    ROUND(SUM(total_amount), 2)                         AS total_revenue,
    ROUND(AVG(total_amount), 2)                         AS avg_revenue_per_trip

FROM {{ ref('stg_yellow_trips') }}

GROUP BY pickup_date, pickup_day_of_week
ORDER BY pickup_date
