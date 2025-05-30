# Databricks notebook source
# MAGIC %md
# MAGIC # Products Data Cleaning Pipeline: Bronze Layer to Silver Layer
# MAGIC
# MAGIC This Pipeline processes the raw products dataset through comprehensive cleansing and standardization operations, including:
# MAGIC
# MAGIC - Identification of null values and duplicate records
# MAGIC - Verification of Product ID uniqueness
# MAGIC - Standardization of Color field with consistent default values
# MAGIC - Recognition and handling of size-accessory relationships within product hierarchy
# MAGIC - Replacing values in the SubCategory field
# MAGIC - Standardization of text fields through consistent formatting
# MAGIC - Saving the Cleaned and Enriched Products Data to the Silver Layer.

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
bronze_products_path = get_bronze_path("products")
silver_products_path = get_silver_path("products")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["silver"]

print(f"Processing Products Data:")
print(f"- Source: {bronze_products_path}")
print(f"- Destination: {silver_products_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, when, lit, count
from pyspark.sql import functions as F

# COMMAND ----------

# Load the Raw Products Data
print(f"Loading bronze Products data...")
tmp_products_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(products_schema) \
  .load(bronze_products_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
products_df = spark.createDataFrame(tmp_products_df.rdd, products_schema)

print(f"Loaded {products_df.count()} Products Data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# DispPay basic statistics
print("Products Data Summary:")
display(products_df.limit(5))

# COMMAND ----------

# Display schema
print("products Data Schema:")
products_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identification of Null Values and Duplicate Records

# COMMAND ----------

# Check for Null Values
print("\nNull Value Counts in Products Data:")
for column in products_df.columns:
    null_count = products_df.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count}")

# COMMAND ----------

# Check for Duplicate Records
duplicate_count = products_df.count() - products_df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"Duplicate Records: {duplicate_count}")
else:
    print("No Duplicate Records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation of Product ID Uniqueness

# COMMAND ----------

# Check for Unique Product IDs
duplicate_prodid_count = products_df.count() - products_df.dropDuplicates(["ProductID"]).count()
if duplicate_prodid_count > 0:
    print(f"Duplicate Product IDs: {duplicate_prodid_count}")
else:
    print("No Duplicate Product IDs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Color and Field with Consistent Default Values

# COMMAND ----------

# Replace nulls in Color column with "Not Specified"
products_df = products_df.withColumn(
    "Color", 
    when(col("Color").isNull(), lit("Not Specified")).otherwise(col("Color"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recognition and Handling of Size-Accessory Relationships within Product Hierarchy

# COMMAND ----------

# Identifying Sub-Categories where Size is Null
nullsizes = products_df.filter(F.col("Sizes").isNull()) \
             .select("SubCategory") \
             .distinct()

display(nullsizes)

# COMMAND ----------

# Replace nulls in Sizes column with "One Size"
products_df = products_df.withColumn(
    "Sizes", 
    when(col("Sizes").isNull(), lit("One Size")).otherwise(col("Sizes"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replacing Values in the SubCategory Field

# COMMAND ----------

# Replacing specific values in the SubCategory column
products_df = products_df.replace(["Girl and Boy (1-5 years, 6-14 years)"],["Children(1-14 years)"], "SubCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Text Fields through Consistent Formatting

# COMMAND ----------

# Standardize Column Names (Strip Whitespace and Convert to Lowercase)
columns_to_standardize = ["Category", "SubCategory", "Color"]
products_df = standardize_columns(products_df, columns_to_standardize)

# COMMAND ----------

display(products_df)

# COMMAND ----------

print("Products Data Cleaning Pipeline Complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer
# MAGIC Save the Cleaned and Enriched Products Data to the Silver Layer.

# COMMAND ----------

# Save as Delta format in the Silver Layer
print(f"Writing {products_df.count()} Products to Silver Layer: {silver_products_path}")

# Delete existing Delta files
dbutils.fs.rm(silver_products_path, recurse=True)

# Apply Delta optimizations
products_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(silver_products_path)

print(f"Successfully wrote Products Data to Silver Layer: {silver_products_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Silver Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the silver data
tmp_silver_products_df = spark.read.format(file_format).load(silver_products_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
silver_products_df = spark.createDataFrame(tmp_silver_products_df.rdd, products_schema)

# Compare record counts
bronze_count = products_df.count()
silver_count = silver_products_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Silver record count: {silver_count}")
print(f"Records match: {bronze_count == silver_count}")

# Display sample from silver layer
print("Sample data from silver layer:")
display(silver_products_df.limit(5))


# COMMAND ----------

silver_products_df.printSchema()
