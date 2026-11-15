
from pyspark.sql import SparkSession
# Imports SparkSession.

from pyspark.sql.functions import col, when, lit
# Imports Spark SQL functions.

import time
# Used to pause the program.

spark = (SparkSession.builder
         .appName("FixedSpark")
         .master("local[4]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
# Creates Spark session using 4 CPU cores and 8 shuffle partitions.

spark.sparkContext.setLogLevel("WARN")
# Reduces unnecessary messages.

df = spark.range(5_000_000)
# Creates 5 million records.

df = df.withColumn(
    "key",
    when(col("id") < 4_500_000, lit(0))
    .otherwise((col("id") % 999) + 1)
)
# Creates skewed data where 90% records have key 0.

df = df.withColumn("fixed_key", col("id") % 10)
# Creates 10 different keys to distribute records.

print("Running fixed Spark job...")
# Displays starting message.

result = df.groupBy("fixed_key").count()
# Groups records using the new key.

result.collect()
# Executes the GROUP BY operation.

print("FIXED JOB COMPLETED!")
# Displays completion message.

print("Open: http://localhost:4040")
# Displays Spark UI address.

time.sleep(120)
# Keeps Spark running for 2 minutes.

spark.stop()
# Stops Spark application.
