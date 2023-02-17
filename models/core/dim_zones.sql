{{ config(materialized='table') }}

select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
    {# this replacement was done because there were different ways of adressing the zone in the table. #}
from {{ ref('taxi_zone_lookup') }}

