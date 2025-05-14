# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transportation: Silver Layer to Gold Layer
# MAGIC
# MAGIC This Pipeline Transports Data from Silver Layer to Gold Layer:
# MAGIC
# MAGIC - Discounts: Copying Discounts Data from Silver Layer and Saving in Gold Layer
# MAGIC - Products: Copying Products Data from Silver Layer and Saving in Gold Layer
# MAGIC - Employees: Copying Employees Data from Silver Layer and Saving in Gold Layer

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
silver_discounts_path = get_silver_path("discounts")
gold_discounts_path = get_gold_path("discounts")

silver_products_path = get_silver_path("products")
gold_products_path = get_gold_path("products")

silver_employees_path = get_silver_path("employees")
gold_employees_path = get_gold_path("employees")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["gold"]

print(f"Processing Discounts Data:")
print(f"- Discounts Source: {silver_discounts_path}")
print(f"- Destination: {gold_discounts_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

print(f"\nProcessing Products Data:")
print(f"- Products Source: {silver_products_path}")
print(f"- Destination: {gold_products_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

print(f"\nProcessing Employees Data:")
print(f"- Employees Source: {silver_employees_path}")
print(f"- Destination: {gold_employees_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Required Data

# COMMAND ----------

# Load Silver layer Discounts data
print(f"Loading Silver Discounts Data...")
tmp_discounts_df = spark.read.format(file_format).load(silver_discounts_path)
discounts_df = spark.createDataFrame(tmp_discounts_df.rdd, discounts_schema2)
print(f"Loaded {discounts_df.count()} Discount records")

# Load Gold layer Products data
print(f"\nLoading Gold Poducts Data...")
tmp_products_df = spark.read.format(file_format).load(silver_products_path)
products_df = spark.createDataFrame(tmp_products_df.rdd, products_schema)
print(f"Loaded {products_df.count()} Product records")

# Load Gold layer Employees data
print(f"\nLoading Gold Employees Data...")
tmp_employees_df = spark.read.format(file_format).load(silver_employees_path)
employees_df = spark.createDataFrame(tmp_employees_df.rdd, employees_schema)
print(f"Loaded {employees_df.count()} Employee records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Profiling

# COMMAND ----------

# Display Discounts Sample Data
display(discounts_df.limit(10))

# COMMAND ----------

# Display Discounts Data Schemas
print("Discounts Data Schema:")
discounts_df.printSchema()

# COMMAND ----------

# Display Products Sample Data
display(products_df.limit(10))

# COMMAND ----------

# Display Products Data Schemas
print("\nProducts Data Schema:")
products_df.printSchema()

# COMMAND ----------

# Display Employees Sample Data
display(employees_df.limit(10))

# COMMAND ----------

# Display Employees Data Schemas
print("\nEmployees Data Schema:")
employees_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer
# MAGIC Save the Data to the Gold Layer.

# COMMAND ----------

# Save as Discounts Delta format in the Gold Layer
print(f"Writing {discounts_df.count()} Discount records to Gold Layer: {gold_discounts_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_discounts_path, recurse=True)

# Apply Delta optimizations
discounts_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_discounts_path)

print(f"Successfully wrote Discounts Data to Gold Layer: {gold_discounts_path}")

# COMMAND ----------

# Save as Products Delta format in the Gold Layer
print(f"Writing {products_df.count()} Product records to Gold Layer: {gold_products_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_products_path, recurse=True)

# Apply Delta optimizations
products_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_products_path)
    
print(f"Successfully wrote Products Data to Gold Layer: {gold_products_path}")

# COMMAND ----------

# Save as Employees Delta format in the Gold Layer
print(f"Writing {employees_df.count()} Employee records to Gold Layer: {gold_employees_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_employees_path, recurse=True)

# Apply Delta optimizations
employees_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_employees_path)
    
print(f"Successfully wrote Employees Data to Gold Layer: {gold_employees_path}")
