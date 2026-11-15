
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import time

# Create Spark Session
spark = (SparkSession.builder
         .appName("SparkSkewPractical")
         .master("local[4]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("Spark application started!")

# Create 5 million records
number_of_records = 5_000_000
df = spark.range(number_of_records)

print("Dataset created!")

# Create skewed data (90% records have key = 0)
skewed_df = df.withColumn(
    "key",
    when(col("id") < number_of_records * 0.90, lit(0))
    .otherwise((col("id") % 999) + 1)
)

print("Skewed dataset created!")

# Display key distribution
print("Key distribution:")
skewed_df.groupBy("key").count() \
    .orderBy(col("count").desc()).show(10)

# Perform GROUP BY operation
print("Starting GROUP BY...")
result = skewed_df.groupBy("key").count()

# Execute the operation
result.collect()

print("GROUP BY completed!")

# Open Spark UI
print("Open Spark UI: http://localhost:4040")

# Keep Spark running for inspection
time.sleep(120)

spark.stop()

# pip install pyspark
# python skew.py
# python fix.py
#java library jdk inbuiter