
import os
import sys

# Set correct Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Remove incorrect Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Create Spark session
builder = (
    SparkSession.builder
    .appName("Delta_Programme")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Spark version:", spark.version)

# Create sample data
data = [
    (1, "krish", 90),
    (2, "ankesh", 88),
    (3, "vicky", 75),
    (4, "nandan", 90),
    (5, "aditya", 88),
    (6, "sohaib", 90)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "salary"])

# Display data
df.show()

spark.stop()

#pip install pyspark==3.5.3
#pip install delta-spark==3.2.0
#python delta.py
#C:\hadoop\bin\winutils.exe
#py -3.11 -m venv .venv
#.\.venv\Scripts\Activate.ps1
