{{ config (materialized='view') }}

select
    -- identifiers
    cast(pulocationid as integer) as pulocationid,
    cast(dolocationid as integer) as dolocationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_number,
    cast(sr_flag as numeric) as sr_flag
from {{ source('hw_staging', 'external_fhv_tripdata') }}
