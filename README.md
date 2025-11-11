# CICD-DATABRICKS-PROJECT
DEMO REPO
from pyspark.sql import SparkSession

# 1. Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("CreateTableExample") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Create a sample DataFrame
data = [(1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 28)]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

# 3. Register as a temporary view
df.createOrReplaceTempView("people_temp")

# Query temporary table
spark.sql("SELECT * FROM people_temp").show()

# 4. Save as a permanent Hive table
df.write.mode("overwrite").saveAsTable("default.people")

# Verify
spark.sql("SELECT * FROM default.people").show()

# 5. Create a table using SQL with partitioning and bucketing
spark.sql("""
CREATE TABLE IF NOT EXISTS default.people_partitioned (
    id INT,
    name STRING,
    age INT
)
USING PARQUET
PARTITIONED BY (age)
CLUSTERED BY (id) INTO 4 BUCKETS
""")

# 6. Load data from CSV and create a table
csv_path = "/path/to/your/file.csv"
csv_df = spark.read.option("header", "true").csv(csv_path)

# Save as table
csv_df.write.mode("overwrite").saveAsTable("default.people_from_csv")

# Query the CSV-based table
spark.sql("SELECT * FROM default.people_from_csv").show()
