from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveTableTransformation") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the Parquet file into a DataFrame
parquet_file_path = "cap/*.parquet"
data = spark.read.parquet(parquet_file_path)
# Transformasi 1 - Rename Columns
data = data.withColumnRenamed("payment_type", "paymentID").withColumnRenamed("trip_type", "tripID")
# Transformasi 2 - Added Columns Vendor Name
data = data.withColumn("Vendor_name", when(data.VendorID == 1, "Creative Mobile Technologies, LLC") \
                        .when(data.VendorID == 2, "VeriFone Inc.") \
                        .otherwise("Unknown"))
# Transformasi 3 - Added Columns Payment Type, Trip Type, Flag Type, Rate Type
data = data.withColumn("payment_type", when(data.paymentID == 1, "Credit card") \
                        .when(data.paymentID == 2, "Cash") \
                        .when(data.paymentID == 3, "No charge") \
                        .when(data.paymentID == 4, "Dispute") \
                        .when(data.paymentID == 5, "Unknown") \
                        .when(data.paymentID == 6, "Voided trip") \
                         )
data = data.withColumn("trip_type", when(data.tripID == 1, "Street-hail").otherwise("Dispatch"))
data = data.withColumn("flag_type", when(data.store_and_fwd_flag == "Y", "store and forward trip").otherwise("not a store and forward trip"))
data = data.withColumn("rate_type", when(data.RatecodeID == 1, "Standard rate") \
                        .when(data.RatecodeID == 2, "JFK") \
                        .when(data.RatecodeID == 3, "Newark") \
                        .when(data.RatecodeID == 4, "Nassau or Westchester") \
                        .when(data.RatecodeID == 5, "Negotiated fare") \
                        .when(data.RatecodeID == 6, "Group ride")            
                        )
# Transformasi 4 - Fill The 99.0 Column with Null 
data = data.fillna({'RatecodeID': 99.0})
# Transformasi 5 - Drop ehall_fee Column
data = data.drop("ehail_fee")
# Transformasi 6 - Change The Data Type
data = data.withColumn("RatecodeID", data.RatecodeID.cast("int"))
data = data.withColumn("tripID", data.tripID.cast("int"))
data = data.withColumn("paymentID", data.paymentID.cast("int"))

# Save File to HDFS
data.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hive/warehouse/data")