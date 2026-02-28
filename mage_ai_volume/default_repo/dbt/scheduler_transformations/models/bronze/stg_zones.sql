with 
base as (
    select * from {{source('raw','taxi_zones')}}
)
select 
    base."LocationID" as location_id,
    base."Borough" as borough,
    base."Zone" as zone_name,
    base.service_zone as service_zone
from base
