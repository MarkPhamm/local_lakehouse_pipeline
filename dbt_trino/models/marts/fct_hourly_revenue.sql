{{ config(
    materialized='table',
    schema='gold',
    properties={
      "format": "'PARQUET'"
    }
) }}

SELECT
    pickup_date,
    pickup_hour,
    COUNT(*)                                AS trip_count,
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    ROUND(AVG(total_amount), 2)             AS avg_revenue,
    ROUND(AVG(trip_distance), 2)            AS avg_distance

FROM {{ ref('stg_yellow_trips') }}

GROUP BY pickup_date, pickup_hour
ORDER BY pickup_date, pickup_hour
