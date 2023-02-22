# Homework: Week 4
<br>

## Question 1. Counting 2019-2020 fact_trips records

After running all models in dbt Cloud, I did the following query on `fact_trips` in BigQuery:

```sql
SELECT COUNT(*) 
FROM `dtc-de-375600.dbt_production.fact_trips` 
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-12-31';
```
<br>

* The answer is: 61,648,442

<br>

## Question 2. Data Distribution 2019-2020 (Green & Yellow) 

I got the answer from looking at the *Pie* graph in Google Looker Studio.

<picture>
<source media= "(prefers-color-scheme: light)" srcset= "https://github.com/dieg0sc/de_homework/blob/main/images/q2_service_distribution.png">
<img alt= "This is the picture for question 2.">
</picture>

<br>
<!-- added this commented line because the line break wasn't working after the picture HTML element. -->
<br>

* The answer is: 89.9/10.1

<br>

## Question 3. Counting 2019 FHV records (staging model)

I need to add the source for `fhv_tripdata` records to the `schema.yml` file first. 
<br>
In my case it will be a different dataset, but if everything's in one place you just need to add a table.

I've added:

```yaml
 - name: hw_staging
   database: dtc-de-375600
   schema: fhv_data

   tables:
     - name: external_fhv_tripdata
```

I'll now create our staging model `stg_fhv_tripdata`. I omitted the variable definition, so there's no reason to include `is_test_run: false` to my  `dbt run` command.

##### stg_fhv_tripdata.sql
```sql
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
```

After running successfully, I'll query the created model using BigQuery, in case there are some records out of the date range. 

```sql
SELECT COUNT(*) 
FROM `dtc-de-375600.dbt_diegos.stg_fhv_tripdata` 
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
```
<br>


* The answer is: 43,244,696


<br>

## Question 4. Counting 2019 FHV records (core model)

Firstly, I'm gonna create the `fact_fhv_trips` fact table.

##### fact_fhv_trips.sql
```sql
{{ config(materialized='table') }}

--this is the subquery
with fhv_data as (
    select *
    from {{ref('stg_fhv_tripdata')}}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_data.pulocationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dolocationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime, 
    fhv_data.dispatching_base_num,
    fhv_data.affiliated_base_number,
    fhv_data.sr_flag
from fhv_data

inner join dim_zones as pickup_zone
on fhv_data.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dolocationid = dropoff_zone.locationid
``` 
Then, I ran  `dbt build --select +fact_fhv_trips` (the model and its dependencies)
<br>
Once the table's been created, I wrote the following query in BQ:

```sql
SELECT COUNT(*) 
FROM `dtc-de-375600.dbt_diegos.fact_fhv_trips` 
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
```
<br>

* The answer is: 22,998,722

<br>

## Question 5. Month with the highest number of FHV rides

After running the project in the Production environment, I used `fact_fhv_trips` as a data source in my Google Looker Studio report.
These are the results I got for each month:

<br>

<picture>
<source media= "(prefers-color-scheme: light)" srcset= "https://github.com/dieg0sc/de_homework/blob/main/images/q5_trips_per_month.png">
<img alt= "This is the picture for question 5.">
</picture>


<br>
<br>

* The answer is: January
