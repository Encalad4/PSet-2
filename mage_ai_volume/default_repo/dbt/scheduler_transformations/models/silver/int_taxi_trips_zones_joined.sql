with 
trips as (
    select * from {{ref('stg_taxi_trips_bronze')}}
),
zones as (
    select * from {{ref('stg_zones')}}
)
select 
    trips."VendorID",
    trips.pickup_ts,
    trips.dropoff_ts,
    coalesce(trips.passenger_count, 0) as passenger_count,
    trips.trip_distance,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.rate_code_id,
    trips.payment_type_id,
    trips.fare_amount,
    trips.tip_amount,
    trips.tolls_amount,
    trips.total_amount,
    trips.trip_type,
    trips.ingest_ts,
    trips.source_month,
    trips.service_type,
    trips.trip_time_minutes,

    


    zones_pickup.zone_name as pickup_location_name, 

    zones_dropoff.zone_name as dropoff_location_name

from trips 
inner join zones as zones_pickup 
    on trips.pickup_location_id = zones_pickup.location_id
inner join zones as zones_dropoff
    on trips.dropoff_location_id = zones_dropoff.location_id
where 
    trips.pickup_location_id is not null 
    and trips.dropoff_location_id is not null
