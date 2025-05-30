# Databricks notebook source
# MAGIC %md
# MAGIC # Discount Data Cleaning Pipeline: Bronze Layer to Silver Layer
# MAGIC
# MAGIC This Pipeline processes the raw discounts dataset through comprehensive cleansing and enrichment operations, including:
# MAGIC - Identification of null values and duplicate records
# MAGIC - Strategic handling of null values in Category and Sub-category fields based on discount pattern analysis
# MAGIC - Replacing values in the SubCategory field
# MAGIC - Assigning Unique ID to each Discount Promotion
# MAGIC - Standardization of text fields with consistent formatting conventions
# MAGIC - Saving the Cleaned and Enriched Discounts Data to the Silver Layer

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
bronze_discounts_path = get_bronze_path("discounts")
silver_discounts_path = get_silver_path("discounts")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["silver"]

print(f"Processing discounts data:")
print(f"- Source: {bronze_discounts_path}")
print(f"- Destination: {silver_discounts_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, count, isnan, isnull, min, max, when, lit, create_map, row_number
from pyspark.sql.window import Window
from itertools import chain

# COMMAND ----------

# Load the Raw Stores Data
print(f"Loading Bronze Discounts Data...")

tmp_discounts_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(discounts_schema) \
  .load(bronze_discounts_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
discounts_df = spark.createDataFrame(tmp_discounts_df.rdd, discounts_schema)

print(f"Loaded {discounts_df.count()} Discounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display basic statistics
print("Discounts Data Summary:")
display(discounts_df.limit(5))

# COMMAND ----------

# Display schema
print("Discounts Data Schema:")
discounts_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identification of Null Values and Duplicate Records

# COMMAND ----------

# Check for Null Values
print("\nNull Value Counts in Discounts Data:")
for column in discounts_df.columns:
    null_count = discounts_df.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count}")

# COMMAND ----------

# Check for Duplicate Records
duplicate_count = discounts_df.count() - discounts_df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"Duplicate Records: {duplicate_count}")
else:
    print("No Duplicate Records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategic Handling of Null Values in Category and Sub-category Fields based on Discount Pattern Analysis

# COMMAND ----------

# Get Min and Max Dates from the Start Column
min_max = discounts_df.agg(
    min("StartDate").alias("min_date"),
    max("StartDate").alias("max_date")
).collect()[0]

min_date = min_max["min_date"]
max_date = min_max["max_date"]

print(f"Discounts Data Date Range: {min_date} to {max_date}")

# COMMAND ----------

# Filter 1: Find Rows Where Category or SubCategory is null/NA
null_category_filter = discounts_df.filter(
    col("Category").isNull() | 
    col("SubCategory").isNull()
)

# Display the First Result
print("Records with Null Category or SubCategory:")
null_category_filter.show(truncate=False)
print(f"Count: {null_category_filter.count()}")

# COMMAND ----------

# Filter 2: Find rows where Discount is either 0.5 or 0.6
discount_filter = discounts_df.filter(
    col("Discount").isin([0.5, 0.6])
)

# Display the second result
print("\nRecords with Discount value of 0.5 or 0.6:")
discount_filter.show(truncate=False)
print(f"Count: {discount_filter.count()}")

# COMMAND ----------

# Check if both filters return the same results
are_same = null_category_filter.exceptAll(discount_filter).count() == 0 and \
           discount_filter.exceptAll(null_category_filter).count() == 0

print(f"\nDo both filters return the same results? {are_same}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Observation: Discount Pattern Analysis
# MAGIC - Upon examining the dataset, we identified a significant pattern in the retailer's discount strategy.
# MAGIC - All discounts of 50% and 60% correspond precisely to null values in both the Category and SubCategory fields.
# MAGIC - This indicates that these particular promotional campaigns—60% for Black Friday and 50% for the holiday season were applied store-wide across all product categories, rather than being targeted to specific merchandise segments.

# COMMAND ----------

# Replace nulls in Category column with "All Categories"
discounts_df = discounts_df.withColumn(
    "Category", 
    when(col("Category").isNull(), lit("All")).otherwise(col("Category"))
)

# COMMAND ----------

# Replace nulls in Sub Category column with "All Sub-Categories"
discounts_df = discounts_df.withColumn(
    "SubCategory", 
    when(col("SubCategory").isNull(), lit("All")).otherwise(col("SubCategory"))
)

# COMMAND ----------

# Cross-Check the Results
print("\nRecords with Discount value of 0.5 or 0.6:")
discounts_df.filter(
    col("Discount").isin([0.5, 0.6])
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replacing Values in the SubCategory Field

# COMMAND ----------

# Replacing specific values in the SubCategory column
discounts_df = discounts_df.replace(["Girl and Boy (1-5 years, 6-14 years)"],["Children(1-14 years)"], "SubCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assigning Unique ID to each Discount Promotion

# COMMAND ----------

# Create a column for DiscountID and assign a unique ID to each discount starting from 1

window_spec = Window.orderBy("Discount")
discounts_df = discounts_df.withColumn("DiscountID", row_number().over(window_spec))

# COMMAND ----------

# Reorder the columns to place DiscountID as the 3rd column
columns = discounts_df.columns
columns.remove("DiscountID")
columns.insert(2, "DiscountID")
discounts_df = discounts_df.select(columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Text Fields with Consistent Formatting

# COMMAND ----------

# Standardize Column Names (Strip Whitespace and Convert to Lowercase)
columns_to_standardize = ["Category", "SubCategory"]
discounts_df = standardize_columns(discounts_df, columns_to_standardize)

# COMMAND ----------

display(discounts_df)

# COMMAND ----------

print("Discounts Data Cleaning Pipeline Complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer
# MAGIC Save the Cleaned and Enriched Discounts Data to the Silver Layer.

# COMMAND ----------

# Save as Delta format in the Silver Layer
print(f"Writing {discounts_df.count()} Discounts to Silver Layer: {silver_discounts_path}")

# Delete existing Delta files
dbutils.fs.rm(silver_discounts_path, recurse=True)

# Apply Delta optimizations
discounts_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(silver_discounts_path)

print(f"Successfully wrote Discounts Data to Silver Layer: {silver_discounts_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Silver Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the silver data
tmp_silver_discounts_df = spark.read.format(file_format).load(silver_discounts_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
silver_discounts_df = spark.createDataFrame(tmp_silver_discounts_df.rdd, discounts_schema2)

# Compare record counts
bronze_count = discounts_df.count()
silver_count = silver_discounts_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Silver record count: {silver_count}")
print(f"Records match: {bronze_count == silver_count}")

# Display sample from silver layer
print("Sample data from silver layer:")
display(silver_discounts_df.limit(5))


# COMMAND ----------

silver_discounts_df.printSchema()
