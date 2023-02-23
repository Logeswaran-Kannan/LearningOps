%python

from pyspark.sql.functions import col, count, mean, stddev, sum, when
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

# Define a widget to pass in the database name as a parameter
db_name = dbutils.widgets.get("db_name")

# Load the table as a DataFrame
table_name = "your_table_name"
df = spark.read.format("delta").option("path", f"/mnt/delta/{db_name}/{table_name}").load()

# Calculate the number of rows in the table
num_rows = df.count()

# Loop over each column and calculate distinct counts, null percentages, and data densities
column_profiles = []
for column in df.columns:
    distinct_count = df.select(column).distinct().count()
    null_count = df.select(when(col(column).isNull(), 1).otherwise(0)).agg(sum(col("CASE WHEN {} IS NULL THEN 1 ELSE 0 END".format(column)))).collect()[0][0]
    null_percentage = null_count / num_rows
    data_density = (num_rows - null_count) / num_rows
    if df.select(column).dtypes[0][1] in ["bigint", "double"]:
        column_mean = df.select(mean(col(column))).collect()[0][0]
        column_stddev = df.select(stddev(col(column))).collect()[0][0]
        column_profile = (db_name, table_name, column, distinct_count, column_mean, column_stddev, null_percentage, data_density, num_rows)
    else:
        column_profile = (db_name, table_name, column, distinct_count, None, None, null_percentage, data_density, num_rows)
    column_profiles.append(column_profile)

# Define the schema for the profiling results
schema = StructType([
    StructField("db_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("distinct_count", IntegerType(), True),
    StructField("column_mean", DoubleType(), True),
    StructField("column_stddev", DoubleType(), True),
    StructField("null_percentage", DoubleType(), True),
    StructField("data_density", DoubleType(), True),
    StructField("num_rows", IntegerType(), True)
])

# Create a DataFrame from the profiling results with the defined schema
profile_df = spark.createDataFrame(column_profiles, schema)

# Write the profiling results to a temporary table
temp_table_name = f"temp_{db_name}_{table_name}_profile"
profile_df.createOrReplaceTempView(temp_table_name)

# Show the number of rows in the table
print(f"Number of Rows: {num_rows}")
