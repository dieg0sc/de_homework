## Question 1. Install Spark and PySpark

Once Spark and PySpark were installed successfully, I executed the following commands
to import the required libraries and start a Spark session:

```python
import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('hw_test') \
    .getOrCreate()
```

After that I checked which port the Spark UI was using `spark.sparkContext.uiWebUrl`.

It was port 4040.

Finally, I executed the version command `spark.version` and got 3.3.2 . 

* The answer is: 3.3.2

<br>

## Question 2. HVFHW June 2021

Firstly, I downloaded the CSV file and unzipped it:

```python
! wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

! gzip -d fhvhv_tripdata_2021-06.csv.gz
```

Then read the file with Spark setting `inferSchema` to `true` 

```python
df = spark.read \
    .options( 
    header = "true", \
    inferSchema = "true", \
            ) \
    .csv('fhvhv_tripdata_2021-06.csv')
```

Which did a pretty decent job at infering the dtypes:

`df.printSchema()`

```pyhton
root
 |-- dispatching_base_num: string (nullable = true)
 |-- pickup_datetime: string (nullable = true)
 |-- dropoff_datetime: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- SR_Flag: string (nullable = true)
 |-- Affiliated_base_number: string (nullable = true)
``` 
I then defined `schema` because we want the pickup and dropoff fields to be *TimestampType*.

```python
schema = types.StructType([
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime", types.TimestampType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
    types.StructField("Affiliated_base_number", types.StringType(), True)
])
```

After that I defined the `DataFrame` for FHVHV finally as it's shown here:

```python
df = spark.read \
    .option( "header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-06.csv')
```

For saving it as a parquet file I did:

```pyhton
df \
    .repartition(12) \
    .write.parquet('data/pq/fhvhv')
```

Finally, I moved into the right directory in the CLI and checked the parquet files inside it using `ls -lh`.


* The answer is: 24 MB

<br>

## Question 3. Count records

I read the saved parquet file we did for Question 2 using `df= spark.read.parquet('data/pq/fhvhv/')`. Then, created a view
(since tables are deprecated from Spark since version 2.0.0 ) with `df.createOrReplaceTempView("fhvhv_data")`. Finally, I ran
the following query:

```sql
spark.sql("""
SELECT
    date_trunc('day', pickup_datetime) AS day,
    COUNT(1) AS number_records
FROM
    fhvhv_data
WHERE
     date(pickup_datetime) == '2021-06-15'
GROUP BY
    1
""").show()
```

* The answer is: 452,470

<br>

## Question 4. Longest trip for each day

I calculated the time interval using `unix_timestamp` PySpark function, which gives us the time in seconds. Then, you can convert it 
to another time-unit by doing the suitable algebraic operation.  
After importing the set of functions with `from pyspark.sql.functions import *` , I executed the following query:

```sql
df_result = spark.sql("""
SELECT
    date_trunc('second', pickup_datetime) AS p_time,
    date_trunc('second', dropoff_datetime) AS d_time,
    ((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))/3600) AS trip_duration
FROM
    fhvhv_data
GROUP BY
    1,2,3
ORDER BY
    3 DESC
""")
```

This gave me the result with *DoubleType* precision. For rounding it to 2 decimal places I used:

```python
from pyspark.sql import types

df_result \
    .withColumn('trip_duration_2d', col('trip_duration').cast(types.DecimalType(38,2))) \
    .drop('trip_duration') \
    .show() 
```

Which can be done inside the SQL query as well with the `ROUND()` statement. I couldn't found a straightforward method for truncating to 2 decimal places.

* The answer is: 66.87 Hours
    
<br>


## Question 5. User Interface

A handy command for checking this in your current Jupyter Notebook is `spark.sparkContext.uiWebUrl`.

* The answer is: Spark UI runs in 4040 local port

<br>

## Question 6. Most frequent pickup location zone

After writing and then reading the parquet file for Taxi Zone Lookup Data

```python
df_zones = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')


df_zones.write.parquet('zones', mode= "overwrite")

df_zones = spark.read.parquet('zones/')
```

I created a temp view 

`df_zones.createOrReplaceTempView("zones")`

And for getting the most frequent pickup zone, I did a simple inner join matching the pickup location ID:

```sql
spark.sql("""
SELECT
    PULocationID AS pickup_location,
    zpu.Zone,
    COUNT(1) AS amount_of_pickups
FROM
    fhvhv_data 
    JOIN zones AS zpu
    ON fhvhv_data.PULocationID = zpu.LocationID
GROUP BY
    1,2
ORDER BY
    3 DESC
""").show()
```
