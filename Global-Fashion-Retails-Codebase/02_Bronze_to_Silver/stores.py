# Databricks notebook source
# MAGIC %md
# MAGIC # Stores Data Cleaning Pipeline: Bronze Layer to Silver Layer
# MAGIC
# MAGIC This Pipeline processes the raw stores dataset through comprehensive cleansing and validation operations, including:
# MAGIC - Identification of null values and duplicate records
# MAGIC - Verification of Store ID uniqueness
# MAGIC - Translation of city, country, and store names to English
# MAGIC - Standardization of text fields with consistent formatting
# MAGIC - Cross-validation of employee counts against the employees dataset
# MAGIC - Saving the Cleaned and Enriched Stores Data to the Silver Layer

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
bronze_stores_path = get_bronze_path("stores")
silver_employees_path = get_silver_path("employees")
silver_stores_path = get_silver_path("stores")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["silver"]

print(f"Processing stores data:")
print(f"- Source: {bronze_stores_path}")
print(f"- Destination: {silver_stores_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, upper, lower, trim, when, regexp_replace, create_map, lit, coalesce, regexp_replace, count as spark_count
from itertools import chain

# COMMAND ----------

# Load the Raw Stores Data
print(f"Loading Bronze Stores Data...")

tmp_stores_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(stores_schema) \
  .load(bronze_stores_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
stores_df = spark.createDataFrame(tmp_stores_df.rdd, stores_schema)

# Load Employees Data from Silver Layer for Employee Count Validation
print(f"Loading Silver Employees Data...")

tmp_employees_df = spark.read.format(file_format).load(silver_employees_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
employees_df = spark.createDataFrame(tmp_employees_df.rdd, employees_schema)

print(f"Loaded {stores_df.count()} Stores and {employees_df.count()} Employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display basic statistics
print("Stores data summary:")
display(stores_df.limit(5))

# COMMAND ----------

# Display schema
print("Stores schema:")
stores_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identification of Null Values and Duplicate Records

# COMMAND ----------

# Check for Null Values
print("\nNull value counts in stores data:")
for column in stores_df.columns:
    null_count = stores_df.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count}")

# COMMAND ----------

# Check for Duplicate Records
duplicate_count = stores_df.count() - stores_df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"Duplicate Records: {duplicate_count}")
else:
    print("No Duplicate Records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification of Store ID uniqueness

# COMMAND ----------

# Check for Duplicate Store IDs
duplicate_storeid_count = stores_df.count() - stores_df.dropDuplicates(["StoreID"]).count()
if duplicate_storeid_count > 0:
    print(f"Duplicate Store IDs: {duplicate_storeid_count}")
else:
    print("No Duplicate Store IDs.")

# COMMAND ----------

# Value distribution for categorical columns
print("\nCountry distribution:")
display(stores_df.groupBy("Country").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Translation of City, Country, and Store Names to English

# COMMAND ----------

# Using map lookup to replace country and city names
country_mappings = create_map(*[lit(x) for x in chain(*[(k, v) for k, v in TRANSLATION_CONFIG["country_replacements"].items()])])

city_mappings = create_map(*[lit(x) for x in chain(*[(k, v) for k, v in TRANSLATION_CONFIG["city_replacements"].items()])])

# Apply translations
stores_df = stores_df.withColumn(
    "Country", 
    coalesce(country_mappings[col("Country")], col("Country"))
)

stores_df = stores_df.withColumn(
    "City", 
    coalesce(city_mappings[col("City")], col("City"))
)

# Apply city translations to Store Name column
for old, new in TRANSLATION_CONFIG["city_replacements"].items():
    stores_df = stores_df.withColumn(
        "StoreName", 
        regexp_replace(col("StoreName"), old, new)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Text Fields through Consistent Formatting

# COMMAND ----------

# Standardize column names (strip whitespace, convert to lowercase, and convert to Title Case)
columns_to_standardize = ["Country", "City", "StoreName"]
stores_df = standardize_columns(stores_df, columns_to_standardize)
stores_df.select(columns_to_standardize).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Validation of Employee Counts against the Employees Dataset
# MAGIC
# MAGIC This section checks and corrects discrepancies between recorded employee counts in the stores table and actual employee counts from the employees table.

# COMMAND ----------

# Calculate actual employee counts per store using Spark aggregation
actual_employee_counts = employees_df.groupBy("StoreID").agg(
    spark_count("EmployeeID").alias("Actual_Employee_Count")
)

# Join with stores dataframe to identify discrepancies
stores_with_counts = stores_df.join(
    actual_employee_counts,
    stores_df["StoreID"] == actual_employee_counts["StoreID"],
    "left"
).select(
    stores_df["*"],
    actual_employee_counts["Actual_Employee_Count"]
)

# Fill missing values with 0 for stores that have no employees in the employees table
stores_with_counts = stores_with_counts.fillna({"Actual_Employee_Count": 0})

# Identify stores with discrepancies
discrepancies = stores_with_counts.filter(
    col("NumberOfEmployees") != col("Actual_Employee_Count")
)

# COMMAND ----------

# Print discrepancy information
discrepancy_count = discrepancies.count()
print(f"Found {discrepancy_count} stores with employee count discrepancies")

if discrepancy_count > 0:
    print("Stores with discrepancies:")
    display(discrepancies.select(
        "StoreID", 
        "StoreName", 
        "NumberOfEmployees", 
        "Actual_Employee_Count"
    ))

# Update the employee counts in the stores dataframe
stores_df = stores_with_counts.withColumn(
    "NumberOfEmployees",
    when(
        col("NumberOfEmployees") != col("Actual_Employee_Count"),
        col("Actual_Employee_Count")
    ).otherwise(col("NumberOfEmployees"))
).drop("Actual_Employee_Count")

print(f"Updated employee counts for {discrepancy_count} stores")

# COMMAND ----------

display(stores_df)

# COMMAND ----------

print("Stores Data Cleaning Pipeline Complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer
# MAGIC Save the Cleaned and Enriched Stores Data to the Silver Layer.

# COMMAND ----------

# Save as Delta format in the Silver Layer
print(f"Writing {stores_df.count()} Stores to Silver Layer: {silver_stores_path}")

# Delete existing Delta files
dbutils.fs.rm(silver_stores_path, recurse=True)

# Apply Delta optimizations
stores_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(silver_stores_path)

print(f"Successfully wrote Stores Data to Silver Layer: {silver_stores_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Silver Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the silver data
tmp_silver_stores_df = spark.read.format(file_format).load(silver_stores_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
silver_stores_df = spark.createDataFrame(tmp_silver_stores_df.rdd, stores_schema)

# Compare record counts
bronze_count = stores_df.count()
silver_count = silver_stores_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Silver record count: {silver_count}")
print(f"Records match: {bronze_count == silver_count}")

# Display sample from silver layer
print("Sample data from silver layer:")
display(silver_stores_df.limit(5))


# COMMAND ----------

silver_stores_df.printSchema()
