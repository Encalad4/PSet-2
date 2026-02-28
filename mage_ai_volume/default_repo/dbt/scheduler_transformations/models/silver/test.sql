-- Check source data for NULLs
select 
    count(*) as total_rows,
    count(case when dropoff_location_id is null then 1 end) as null_dropoff_count
from {{ref('stg_taxi_trips_bronze')}}