# Databricks notebook source
# MAGIC %md
# MAGIC # Transactions Data Cleaning Pipeline: Bronze Layer to Silver Layer
# MAGIC
# MAGIC This Pipeline processes the raw transactions dataset through comprehensive cleansing, and standardization, including:
# MAGIC
# MAGIC - Identification of null values and duplicate records
# MAGIC - Standardization of Color and Size Fields with Consistent Default Values
# MAGIC - Standardization of text fields through consistent formatting
# MAGIC - Saving the Cleaned and Enriched Transactions Data to the Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import All Utilities
# MAGIC
# MAGIC Load utility functions that are shared across processing notebooks.

# COMMAND ----------

# MAGIC %run ../01_Utilities/config

# COMMAND ----------

# MAGIC %run ../01_Utilities/common_functions

# COMMAND ----------

# MAGIC %run ../01_Utilities/data_quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Define input and output paths and processing parameters.

# COMMAND ----------

# Input and output paths using mount points
bronze_transactions_path = get_bronze_path("transactions")
silver_transactions_path = get_silver_path("transactions")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["silver"]

print(f"Processing Transactions Data:")
print(f"- Source: {bronze_transactions_path}")
print(f"- Destination: {silver_transactions_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, upper, lower, trim, when, regexp_replace, create_map, lit, coalesce, regexp_replace, count
from itertools import chain

# COMMAND ----------

# Load the Raw Transactions Data
print(f"Loading bronze Transactions Data...")
tmp_transactions_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(transactions_schema) \
  .load(bronze_transactions_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
transactions_df = spark.createDataFrame(tmp_transactions_df.rdd, transactions_schema)

transactions_df.cache()
transactions_count = transactions_df.count()

print(f"Loaded {transactions_count} Transactions Data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display basic statistics
print("Transactions Data Summary:")
display(transactions_df.limit(5))

# COMMAND ----------

# Display schema
print("Transactions Data Schema:")
transactions_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identification of Null Values and Duplicate Records

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

# Get total row count once
total_rows = transactions_df.count()

# Calculate all null counts in a single DataFrame operation
null_counts = transactions_df.select([
    count(when(col(c).isNull() | (col(c).cast('double').isNotNull() & isnan(col(c).cast('double'))), c)).alias(c) 
    for c in transactions_df.columns
]).collect()[0]

# Display results
print(f"\nNull Value Analysis for {total_rows:,} rows:")
for column in transactions_df.columns:
    null_count = null_counts[column]
    null_percentage = round(null_count / total_rows * 100, 2)
    print(f"  {column}: {null_count:,} nulls ({null_percentage}%)")

# COMMAND ----------

# Check for Duplicate Records
duplicate_count = transactions_df.count() - transactions_df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"Duplicate Records: {duplicate_count}")
    transactions_df = transactions_df.dropDuplicates()
    print(f"\nRemoved {duplicate_count} duplicate records.")
else:
    print("No Duplicate Records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Color and Size Fields with Consistent Default Values

# COMMAND ----------

# Replace nulls in Color column with "Not Specified"
transactions_df = transactions_df.withColumn(
    "Color", 
    when(col("Color").isNull(), lit("Not Specified")).otherwise(col("Color"))
)

# COMMAND ----------

# Replace nulls in Size column with "One Size"
transactions_df = transactions_df.withColumn(
    "Size", 
    when(col("Size").isNull(), lit("One Size")).otherwise(col("Size"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Text Fields through Consistent Formatting

# COMMAND ----------

# Standardize Column Names (Strip Whitespace, Convert to Lowercase and then Convert to Title Case)
columns_to_standardize = ["Color","TransactionType", "PaymentMethod"]
transactions_df = standardize_columns(transactions_df, columns_to_standardize)

# COMMAND ----------

display(transactions_df)

# COMMAND ----------

print("Transactions Data Cleaning Pipeline Complete")

# COMMAND ----------

trasactions_df.cache()
transactions_count = transactions_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer
# MAGIC Save the Cleaned and Enriched Transactions Data to the Silver Layer.

# COMMAND ----------

# Save as Delta format in the Silver Layer
print(f"Writing {transactions_count} transactions to Silver Layer: {silver_transactions_path}")

# Delete existing Delta files
dbutils.fs.rm(silver_transactions_path, recurse=True)

# Apply Delta optimizations
transactions_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(silver_transactions_path)

print(f"Successfully wrote {transactions_count} transactions Data to Silver Layer: {silver_transactions_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Silver Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the silver data
tmp_silver_transactions_df = spark.read.format(file_format).load(silver_transactions_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
silver_transactions_df = spark.createDataFrame(tmp_silver_transactions_df.rdd, transactions_schema)
silver_transactions_df.cache()

# Compare record counts
bronze_count = transactions_count
silver_count = silver_transactions_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Silver record count: {silver_count}")
print(f"Records match: {bronze_count == silver_count}")

# Display sample from silver layer
print("Sample data from silver layer:")
display(silver_transactions_df.limit(5))

# COMMAND ----------

silver_transactions_df.printSchema()
