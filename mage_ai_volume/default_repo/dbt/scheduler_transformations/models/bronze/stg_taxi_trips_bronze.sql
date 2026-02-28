with 
base as (
    select * from {{ source('raw', 'taxi_trips') }}
)
select 
    base."VendorID", 
    base.lpep_pickup_datetime::timestamp as pickup_ts,
    base.lpep_dropoff_datetime::timestamp as dropoff_ts,
    base.passenger_count::int as passenger_count,
    base.trip_distance::numeric(10,2) as trip_distance,
    base."PULocationID"::int as pickup_location_id,
    base."DOLocationID"::int as dropoff_location_id,
    base."RatecodeID"::int as rate_code_id,
    base.payment_type::int as payment_type_id,
    base.fare_amount::numeric(15,2) as fare_amount,
    base.tip_amount::numeric(15,2) as tip_amount,
    base.tolls_amount::numeric(15,2) as tolls_amount,
    base.total_amount::numeric(15,2) as total_amount,
    base.trip_type::int as trip_type,
    base.ingest_ts::timestamp as ingest_ts,
    base.source_month,
    base.service_type,
    extract(epoch from (base.lpep_dropoff_datetime::timestamp - base.lpep_pickup_datetime::timestamp)) / 60.0 as trip_time_minutes
from base
-- Filter out rows with values that would exceed numeric(10,2)
where 
    base.fare_amount::numeric <= 99999999.99 AND
    base.tip_amount::numeric <= 99999999.99 AND
    base.tolls_amount::numeric <= 99999999.99 AND
    base.total_amount::numeric <= 99999999.99