from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveTableTransformation") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the Parquet file into a DataFrame
parquet_file_path = "hdfs://localhost:9000/user/hive/warehouse/data/*.parquet"
data = spark.read.parquet(parquet_file_path)

# Create Temp View SparkSQL
data.createOrReplaceTempView("green_trip")

# Create Dimention 1 we named vendor table
dim_table1 = spark.sql("""
    SELECT DISTINCT(VendorID), Vendor_name
    FROM green_trip
    ORDER BY VendorID ASC
    """)

# Create Dimention 2 we named passenger_payment table
dim_table2 = spark.sql("""
    SELECT DISTINCT(paymentID), payment_type
    FROM green_trip
    WHERE paymentID IS NOT NULL
    ORDER BY paymentID ASC
    """)

# Create Dimention 3 we named passenger_trip table
dim_table3 = spark.sql("""
    SELECT DISTINCT(tripID), trip_type
    FROM green_trip
    WHERE tripID IS NOT NULL
    ORDER BY tripID ASC
    """)

# Create Dimention 4 we named rate_trip table
dim_table4 = spark.sql("""
    SELECT DISTINCT(CAST(RatecodeID AS double)) AS RatecodeID, rate_type
    FROM green_trip
    WHERE RatecodeID != 99.0
    ORDER BY RatecodeID ASC
    """)

# Create Dimention 5 we named flag_trip table
dim_table5 = spark.sql("""
    SELECT DISTINCT(store_and_fwd_flag), flag_type
    FROM green_trip
    WHERE store_and_fwd_flag IS NOT NULL
    ORDER BY store_and_fwd_flag ASC
    """)

# Create Fact Table named green_passenger table
fact_table = spark.sql("""
    SELECT VendorID,
    passenger_count,
    lpep_pickup_datetime, 
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    tripID,
    trip_distance,
    RatecodeID,
    PULocationID,
    DOLocationID,
    tip_amount,
    fare_amount,
    tolls_amount,
    extra,
    mta_tax,
    total_amount,
    paymentID,
    improvement_surcharge,
    congestion_surcharge
    FROM green_trip
    """)


# print dimtable
print(dim_table1.show())
print(dim_table2.show())
print(dim_table3.show())
print(dim_table4.show())
print(dim_table5.show())
print(fact_table.show(10))

# Save Table
dim_table1.write.mode("overwrite").saveAsTable("staging.vendor")
dim_table2.write.mode("overwrite").saveAsTable("staging.passenger_payment")
dim_table3.write.mode("overwrite").saveAsTable("staging.passenger_trip")
dim_table4.write.mode("overwrite").saveAsTable("staging.rate_trip")
dim_table5.write.mode("overwrite").saveAsTable("staging.flag_trip")
fact_table.write.mode("overwrite").saveAsTable("staging.green_passenger")

spark.stop()