{{ config(
    materialized='table',
    schema='silver',
    properties={
      "format": "'PARQUET'"
    }
) }}

SELECT
    vendorid                                        AS vendor_id,
    tpep_pickup_datetime                            AS pickup_datetime,
    tpep_dropoff_datetime                           AS dropoff_datetime,
    CAST(passenger_count AS INTEGER)                AS passenger_count,
    trip_distance,
    CAST(ratecodeid AS INTEGER)                     AS rate_code_id,
    store_and_fwd_flag,
    pulocationid                                    AS pickup_location_id,
    dolocationid                                    AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    -- Derived columns
    date(tpep_pickup_datetime)                      AS pickup_date,
    hour(tpep_pickup_datetime)                      AS pickup_hour,
    day_of_week(tpep_pickup_datetime)               AS pickup_day_of_week,
    date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_seconds

FROM {{ source('raw', 'yellow_trips') }}

WHERE
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND trip_distance > 0
    AND fare_amount > 0
    AND tpep_pickup_datetime < tpep_dropoff_datetime
