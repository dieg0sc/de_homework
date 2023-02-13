# Homework: Week 3

#### SETUP:
Dataset ID: `fhv_data` <br>
External Table name: `external_fhv_tripdata` <br>
BQ Table name: `fhv_2019_data` <br>

<br>

## Question 1. Counting FHV 2019 records

This is the query I executed:

```sql
SELECT COUNT(*) 
FROM `dtc-de-375600.fhv_data.fhv_2019_data`;
```
(can be used with the External Table as well)

* The answer is: 43,244,696

<br>

## Question 2. Estimated amount of data to be read

For the external table:
```sql
SELECT DISTINCT(Affiliated_base_number)
FROM `dtc-de-375600.fhv_data.external_fhv_tripdata`;
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;►Estimated amount: 0 B


For the BQ table:
```sql
SELECT DISTINCT(Affiliated_base_number)
FROM `dtc-de-375600.fhv_data.fhv_2019_data`;
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;►Estimated amount: 317.94 MB

<br>

* The answer is: 0 MB for the External Table and 317.94MB for the BQ Table

<br>

## Question 3. Null values for PUlocationID and DOlocationID

This is the query I used. Using BQ Table yields the same result.
```sql
SELECT COUNT(*) 
FROM `dtc-de-375600.fhv_data.external_fhv_tripdata` 
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

* The answer is: 717,748

<br>

## Question 4. Best strategy for table optimisation

Since the queries will always filter by a single column (*pickup_datetime* , which is also
a ***time-unit*** column), and order the data by *Affiliated_base_number* (whose dtype ***string*** is clustering-supported),
I think partitioning by *pickup_datetime* and clustering on *Affiliated_base_number* would be the most suitable thing
to do.

* The answer is: Partition by pickup_datetime, Cluster on affiliated_base_number

<br>

## Question 5. Performance comparison between partitioned and non-partitioned tables

Creating the partitioned-clustered table:
```sql
CREATE OR REPLACE TABLE `dtc-de-375600.fhv_data.fhv_2019_data_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `dtc-de-375600.fhv_data.external_fhv_tripdata`;
```

For retrieving the distinct *Affiliated_base_number* between **2019/03/01** and **2019/03/31** I queried:
```sql
SELECT DISTINCT(Affiliated_base_number)
FROM `dtc-de-375600.fhv_data.fhv_2019_data_partitioned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;►Estimated amount: ~23.05 MB

For the already created BQ Table it would be:
```sql
SELECT DISTINCT(Affiliated_base_number)
FROM `dtc-de-375600.fhv_data.fhv_2019_data`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;►Estimated amount: 647.87 MB

<br>

* The answer is: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

<br>

## Question 6. Data storage of an external table

* The answer is: GCP Bucket

<br>

## Question 7. Best practices 

* The answer is: True 

