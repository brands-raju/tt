
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("Delta_Programme")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create sample data
data = [
    (1, "krish", 90),
    (2, "ankesh", 88),
    (3, "vicky", 75),
    (4, "nandan", 90),
    (5, "aditya", 88),
    (6, "sohaib", 90)
]

df = spark.createDataFrame(data, ["id", "name", "salary"])

# Create Delta table
path = "delta_table"

df.write.format("delta").mode("overwrite").save(path)

print("Delta table created!")

# Read Delta table
delta_df = spark.read.format("delta").load(path)
delta_df.show()

# Update salary (ACID transaction)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, path)

delta_table.update(
    condition="id = 3",
    set={"salary": "95"}
)

print("After update:")
delta_table.toDF().show()

# Show transaction history
delta_table.history().show()

# Time Travel - Read previous version
print("Previous version:")
old_df = spark.read.format("delta").option(
    "versionAsOf", 0
).load(path)

old_df.show()

spark.stop()
