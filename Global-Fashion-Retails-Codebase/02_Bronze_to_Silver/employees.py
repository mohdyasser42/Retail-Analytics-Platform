# Databricks notebook source
# MAGIC %md
# MAGIC # Employees Data Cleaning Pipeline: Bronze Layer to Silver Layer
# MAGIC
# MAGIC This Pipeline processes raw employee data through comprehensive cleansing and standardization operations, including:
# MAGIC
# MAGIC  - Identification of null values and duplicate records
# MAGIC  - Validation of Employee ID uniqueness
# MAGIC  - Removal of academic titles and abbreviations from name fields
# MAGIC  - Translation of employee names to English using Azure Translator Service
# MAGIC  - Standardization of text fields through consistent formatting
# MAGIC  - Saving the Cleaned and Enriched Employees Data to the Silver Layer.

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
bronze_employees_path = get_bronze_path("employees")
silver_employees_path = get_silver_path("employees")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["silver"]

print(f"Processing Employees Data:")
print(f"- Source: {bronze_employees_path}")
print(f"- Destination: {silver_employees_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, upper, lower, trim, when, regexp_replace, create_map, lit, coalesce, regexp_replace, count
from itertools import chain
from pyspark.sql.types import StringType
import requests, uuid
from pyspark.sql import DataFrame
import time

# COMMAND ----------

# Load the Raw Employees Data
print(f"Loading bronze employees data...")
tmp_employees_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(employees_schema) \
  .load(bronze_employees_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
employees_df = spark.createDataFrame(tmp_employees_df.rdd, employees_schema)

print(f"Loaded {employees_df.count()} Employees Data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display basic statistics
print("Employees Data Summary:")
display(employees_df.limit(5))

# COMMAND ----------

# Display schema
print("Employees Data Schema:")
employees_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identification of Null Values and Duplicate Records

# COMMAND ----------

# Check for Null Values
print("\nNull Value Counts in Employees Data:")
for column in employees_df.columns:
    null_count = employees_df.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count}")

# COMMAND ----------

# Check for Duplicate Records
duplicate_count = employees_df.count() - employees_df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"Duplicate Records: {duplicate_count}")
else:
    print("No Duplicate Records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation of Employee ID Uniqueness

# COMMAND ----------

# Check for Unique Employee IDs
duplicate_empid_count = employees_df.count() - employees_df.dropDuplicates(["EmployeeID"]).count()
if duplicate_empid_count > 0:
    print(f"Duplicate Employee IDs: {duplicate_empid_count}")
else:
    print("No Duplicate Employee IDs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removal of Academic Titles and Abbreviations from Name fields

# COMMAND ----------

# Remove Academic Titles and Abbreviations from the Name Column 
employees_df = employees_df.withColumn(
    "Name", 
    trim(regexp_replace(col("Name"), name_pattern, ''))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Translation of Employee Names to English Using Azure Translator

# COMMAND ----------

# Azure Translator credentials
key = Azure_Translator_Key
endpoint = Azure_Translator_endpoint
location = Azure_Translator_location

def batch_translate_api(texts, batch_size=100, max_retries=3, sleep_between_batches=1):
    """
    Translates a list of texts using Azure Translation API in batches
    Returns a dictionary mapping original text to translated text
    
    Args:
        texts: List of texts to translate
        batch_size: Number of texts to process in each API call (max 100)
        max_retries: Number of retries for failed API calls
        sleep_between_batches: Time to sleep between batches to avoid rate limiting
    """
    if not texts:
        return {}
    
    # Remove duplicates and None values
    unique_texts = list(set([t for t in texts if t]))
    if not unique_texts:
        return {}
    
    # Prepare translation API parameters
    path = '/translate'
    constructed_url = endpoint + path
    
    params = {
        'api-version': '3.0',
        'from': 'zh-Hans',
        'to': 'en'
    }
    
    headers = {
        'Ocp-Apim-Subscription-Key': key,
        'Ocp-Apim-Subscription-Region': location,
        'Content-type': 'application/json'
    }
    
    # Process in batches
    translations = {}
    for i in range(0, len(unique_texts), batch_size):
        batch = unique_texts[i:i+batch_size]
        body = [{'text': text} for text in batch]
        
        # Try with retries
        for retry in range(max_retries):
            try:
                # Generate a new trace ID for each request
                headers['X-ClientTraceId'] = str(uuid.uuid4())
                
                # Make API call
                response = requests.post(constructed_url, params=params, headers=headers, json=body)
                response.raise_for_status()
                
                result = response.json()
                
                # Process results
                for j, item in enumerate(result):
                    original_text = batch[j]
                    translated_text = item['translations'][0]['text'] if 'translations' in item and item['translations'] else original_text
                    translations[original_text] = translated_text
                
                # Successful batch, break retry loop
                break
                
            except Exception as e:
                print(f"Translation error on batch {i//batch_size + 1}, retry {retry + 1}: {str(e)}")
                if retry == max_retries - 1:
                    # Last retry failed, use original text
                    for text in batch:
                        translations[text] = text
                time.sleep(1)  # Wait before retry
        
        # Sleep between batches to avoid rate limiting
        if i + batch_size < len(unique_texts):
            time.sleep(sleep_between_batches)
            
        # Print progress
        print(f"Processed {i + len(batch)}/{len(unique_texts)} unique names")
    
    return translations

def translate_chinese_employee_names(df, batch_size=100):
    """
    Process Chinese employee names in the DataFrame with improved batching
    
    Args:
        df: Input DataFrame
        batch_size: Size of batches for translation API calls
    """
    # 1. Extract and count Chinese employees (StoreIDs 6-10)
    chinese_stores_filter = col("StoreID").isin(6, 7, 8, 9, 10)
    chinese_employees_count = df.filter(chinese_stores_filter).count()
    print(f"Total Chinese store employees: {chinese_employees_count}")
    
    if chinese_employees_count == 0:
        print("No Chinese store employees to translate")
        return df
    
    # 2. Get distinct Chinese names only to minimize API calls
    chinese_names_df = df.filter(chinese_stores_filter).select("Name").distinct()
    unique_chinese_names_count = chinese_names_df.count()
    print(f"Unique Chinese store employee names: {unique_chinese_names_count}")
    
    # 3. Collect names for translation (this will be efficient since we're only getting distinct names)
    chinese_names = [row.Name for row in chinese_names_df.collect() if row.Name]
    print(f"Found {len(chinese_names)} non-null unique Chinese employee names")
    
    # 4. Create translation mapping with better batching
    print("Starting translation in batches...")
    name_translations = batch_translate_api(chinese_names, batch_size=batch_size)
    print(f"Completed translations: {len(name_translations)} names")
    
    # 5. Create a mapping DataFrame for efficient joins
    translation_entries = [(name, name_translations.get(name, name)) for name in name_translations]
    if not translation_entries:
        print("No translations were generated")
        return df
        
    # 6. Create mapping DataFrame
    try:
        translation_df = spark.createDataFrame(translation_entries, ["original_name", "translated_name"])
        
        # 7. Join with main DataFrame and apply translations
        result_df = df.join(
            translation_df,
            (df["Name"] == translation_df["original_name"]) & chinese_stores_filter,
            "left_outer"
        )
        
        # 8. Create the final Name column using coalesce to handle the join
        final_df = result_df.withColumn(
            "Name", 
            when(chinese_stores_filter, 
                when(col("translated_name").isNotNull(), col("translated_name")).otherwise(col("Name"))
            ).otherwise(col("Name"))
        ).drop("original_name", "translated_name")
        
        return final_df
        
    except Exception as e:
        print(f"Error creating mapping DataFrame: {str(e)}")
        # Return original DataFrame if we couldn't translate
        return df

# Use a smaller batch size and add progress tracking
employees_df = translate_chinese_employee_names(employees_df, batch_size=50)

# Verify results
non_null_chinese = employees_df.filter(col("StoreID").isin(6, 7, 8, 9, 10) & col("Name").isNotNull()).count()
total_chinese = employees_df.filter(col("StoreID").isin(6, 7, 8, 9, 10)).count()
print(f"Chinese store employees with non-null names: {non_null_chinese} out of {total_chinese}")

# Show sample of translated names
display(employees_df.filter(col("StoreID").isin(6, 7, 8, 9, 10)).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Text Fields through Consistent Formatting

# COMMAND ----------

# Standardize Column Names (Strip Whitespace, Convert to Lowercase then Convert to Title Case)
columns_to_standardize = ["Name","Position"]
employees_df = standardize_columns(employees_df, columns_to_standardize)

# COMMAND ----------

display(employees_df)

# COMMAND ----------

print("Employees Data Cleaning Pipeline Complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer
# MAGIC Save the Cleaned and Enriched Employees Data to the Silver Layer.

# COMMAND ----------

# Save as Delta format in the Silver Layer
print(f"Writing {employees_df.count()} Employees to Silver Layer: {silver_employees_path}")

# Delete existing Delta files
dbutils.fs.rm(silver_employees_path, recurse=True)

# Apply Delta optimizations
employees_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(silver_employees_path)

print(f"Successfully wrote Employees Data to Silver Layer: {silver_employees_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Silver Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the silver data
tmp_silver_employees_df = spark.read.format(file_format).load(silver_employees_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
silver_employees_df = spark.createDataFrame(tmp_silver_employees_df.rdd, employees_schema)

# Compare record counts
bronze_count = employees_df.count()
silver_count = silver_employees_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Silver record count: {silver_count}")
print(f"Records match: {bronze_count == silver_count}")

# Display sample from silver layer
print("Sample data from silver layer:")
display(silver_employees_df.limit(5))


# COMMAND ----------

silver_employees_df.printSchema()
