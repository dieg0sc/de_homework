# Homework: Week 1

## Question 1. Knowing docker tags

I used `docker build --help`

* The answer is: `--iidfile string`

## Question 2. Understanding docker first run

I ran `docker run -i python:3.9 bash`

* The answer is: 3

## Question 3. Count records

SQL-query I used:

```sql
SELECT
  CAST(lpep_pickup_datetime as DATE) as "start_day",
  CAST(lpep_dropoff_datetime as DATE) as "end_day",
  COUNT(1) as "count"
FROM
  green_taxi_trips t
GROUP BY
  CAST(lpep_dropoff_datetime as DATE),
  CAST(lpep_pickup_datetime as DATE)
ORDER BY "start_day" ASC;
```

* The answer is: 20530

## Question 4. Largest trip for each day

```sql
SELECT
  CAST(lpep_pickup_datetime as DATE) as "day",
  MAX(Trip_distance)
FROM
  green_taxi_trips t
GROUP BY
  CAST(lpep_pickup_datetime as DATE)
ORDER BY "day" ASC;
```

* The answer is: 2019-01-15

## Question 5. The number of passengers

I found 2 ways of obtaining the result. 

1. The first one is by consulting one amount of passengers at a time. So, for 2 passengers only:

```sql
SELECT
  CAST(lpep_pickup_datetime as DATE) as "day",
  COUNT(t.Passenger_count) as "2p"
FROM
  green_taxi_trips t
WHERE
  t.Passenger_count=2
GROUP BY
  CAST(lpep_pickup_datetime as DATE)
ORDER BY "day" ASC;
```
And for 3 passengers:

```sql
SELECT
  CAST(lpep_pickup_datetime as DATE) as "day",
  COUNT(t.Passenger_count) as "3p"
FROM
  green_taxi_trips t
WHERE
  t.Passenger_count=3
GROUP BY
  CAST(lpep_pickup_datetime as DATE)
ORDER BY "day" ASC;
```
2. The second way is a bit odd, but you can obtain both results with just one query.

```sql
SELECT
  CAST(lpep_pickup_datetime as DATE) as "day",
  CAST(Passenger_count=2 as int) as "2p",
  CAST(Passenger_count=3 as int) as "3p",
  COUNT(1)
FROM
  green_taxi_trips t
GROUP BY
  CAST(lpep_pickup_datetime as DATE),
  CAST(Passenger_count=2 as int),
  CAST(Passenger_count=3 as int)
ORDER BY "day" ASC;
```

It returns a table with 3 possible combinations of **_2p_** and **_3p_** columns for each day. Interpreting 0 as _False_ and 1 as _True_, one can see that 0 in **both** columns refers to the remaining trips with neither 2 nor 3 passengers, and 0-1,1-0 to **only** 2 or 3 passengers, respectively.

* The answer is:  2: 1282 ; 3: 254

## Question 6. Largest tip

```sql
SELECT
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  zdo."Zone",
  MAX(Tip_amount) as "max_tip"
FROM
  green_taxi_trips t JOIN g_zones zpu ON t."PULocationID" = zpu."LocationID"
                     JOIN g_zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
  zpu."Zone"='Astoria'
GROUP BY 
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  zdo."Zone"
ORDER BY "max_tip" DESC;
```
* The answer is: Long Island City/Queens Plaza
