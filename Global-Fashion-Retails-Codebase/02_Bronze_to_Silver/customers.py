# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Data Cleaning Pipeline: Bronze Layer to Silver Layer
# MAGIC
# MAGIC This Pipeline processes the raw customers dataset through comprehensive cleansing, standardization and enrichment operations, including:
# MAGIC
# MAGIC - Identification of null values and duplicate records
# MAGIC - Verification of Customer ID uniqueness
# MAGIC - Removal of Academic Titles and Abbreviations from Name fields
# MAGIC - Selective translation of Country and City to English
# MAGIC - Translation of Customers Names to English Using Azure Translator Service
# MAGIC - Validation of Email format integrity
# MAGIC - Normalization of Telephone numbers through consistent formatting
# MAGIC - Standardization of JobTitle information with appropriate default values
# MAGIC - Standardization of text fields through consistent formatting
# MAGIC - Saving the Cleaned and Enriched Customers Data to the Silver Layer.

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
bronze_customers_path = get_bronze_path("customers")
silver_customers_path = get_silver_path("customers")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["silver"]

print(f"Processing customers Data:")
print(f"- Source: {bronze_customers_path}")
print(f"- Destination: {silver_customers_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, upper, lower, trim, when, regexp_replace, create_map, lit, coalesce, regexp_replace, count, desc, regexp_extract
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from itertools import chain
import requests, uuid
import time

# COMMAND ----------

# Load the Raw Customers Data
print(f"Loading bronze Customers Data...")
tmp_customers_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(customers_schema) \
  .load(bronze_customers_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
customers_df = spark.createDataFrame(tmp_customers_df.rdd, customers_schema)

customers_df.cache()
customers_count = customers_df.count()

print(f"Loaded {customers_count} Customers Data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display basic statistics
print("Customers Data Summary:")
display(customers_df.limit(5))

# COMMAND ----------

# Display schema
print("Customers Data Schema:")
customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identification of Null Values and Duplicate Records

# COMMAND ----------

# Check for Null Values
print("\nNull Value Counts in Customers Data:")
for column in customers_df.columns:
    null_count = customers_df.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count}")

# COMMAND ----------

# Check for Duplicate Records
duplicate_count = customers_df.count() - customers_df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"Duplicate Records: {duplicate_count}")
else:
    print("No Duplicate Records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation of Customer ID Uniqueness

# COMMAND ----------

# Check for Unique Customer IDs
duplicate_cusid_count = customers_df.count() - customers_df.dropDuplicates(["CustomerID"]).count()
if duplicate_cusid_count > 0:
    print(f"Duplicate Customer IDs: {duplicate_cusid_count}")
else:
    print("No Duplicate Customer IDs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removal of Academic Titles and Abbreviations from Name fields

# COMMAND ----------

# Remove Academic Titles and Abbreviations from the Name Column 
customers_df = customers_df.withColumn(
    "Name", 
    trim(regexp_replace(col("Name"), name_pattern, ''))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selective Translation of Country and City to English

# COMMAND ----------

# Using Map lookup to Replace Names, City and Country with their Translations
country_mappings = create_map(*[lit(x) for x in chain(*[(k, v) for k, v in TRANSLATION_CONFIG["country_replacements"].items()])])

city_mappings = create_map(*[lit(x) for x in chain(*[(k, v) for k, v in TRANSLATION_CONFIG["city_replacements"].items()])])

# COMMAND ----------

# Apply translations
customers_df = customers_df.withColumn(
    "Country", 
    coalesce(country_mappings[col("Country")], col("Country"))
)

customers_df = customers_df.withColumn(
    "City", 
    coalesce(city_mappings[col("City")], col("City"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Translation of Customers Names to English Using Azure Translator

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
    
    return translations

def translate_chinese_names_batched(df, batch_size=100):
    """
    Process Chinese names in the DataFrame with improved batching
    
    Args:
        df: Input DataFrame
        batch_size: Size of batches for translation API calls
    """
    # 1. Extract and count Chinese customers
    chinese_customers_count = df.filter(col("Country") == "China").count()
    print(f"Total Chinese customers: {chinese_customers_count}")
    
    if chinese_customers_count == 0:
        print("No Chinese customers to translate")
        return df
    
    # 2. Get distinct Chinese names only to minimize API calls
    chinese_names_df = df.filter(col("Country") == "China").select("Name").distinct()
    unique_chinese_names_count = chinese_names_df.count()
    print(f"Unique Chinese names: {unique_chinese_names_count}")
    
    # 3. Collect names for translation (this will be efficient since we're only getting distinct names)
    chinese_names = [row.Name for row in chinese_names_df.collect() if row.Name]
    print(f"Found {len(chinese_names)} non-null unique Chinese names")
    
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
            (df["Name"] == translation_df["original_name"]) & (df["Country"] == "China"),
            "left_outer"
        )
        
        # 8. Create the final Name column using coalesce to handle the join
        final_df = result_df.withColumn(
            "Name", 
            when(col("Country") == "China", 
                when(col("translated_name").isNotNull(), col("translated_name")).otherwise(col("Name"))
            ).otherwise(col("Name"))
        ).drop("original_name", "translated_name")
        
        return final_df
        
    except Exception as e:
        print(f"Error creating mapping DataFrame: {str(e)}")
        # Return original DataFrame if we couldn't translate
        return df

# Use a smaller batch size and add progress tracking
customers_df = translate_chinese_names_batched(customers_df, batch_size=50)

# Verify results
non_null_chinese = customers_df.filter((col("Country") == "China") & col("Name").isNotNull()).count()
total_chinese = customers_df.filter(col("Country") == "China").count()
print(f"Chinese customers with non-null names: {non_null_chinese} out of {total_chinese}")

# Show sample of translated names
display(customers_df.filter(col("Country") == "China").limit(10))

# COMMAND ----------

# Forcing Spark to Execute all the Transformations
customers_df.cache()
customers_df.count()

# COMMAND ----------

# Assuming Chinese names contain Chinese characters
chinese_names_df = customers_df.filter(col("Country") == "China").filter(regexp_extract(col("Name"), "[\u4e00-\u9fff]", 0) != "")
chinese_names_count = chinese_names_df.count()

print(f"Count of Chinese names: {chinese_names_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation of Email Format Integrity

# COMMAND ----------

# check if all rows have '@' in their Email column
all_emails_valid = check_all_emails_valid(customers_df)
print(f"All emails contain '@': {all_emails_valid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalization of Telephone Numbers through Consistent Formatting

# COMMAND ----------

# Extract unique characters other than numbers from the Telephone column
unique_chars_df = customers_df.select(regexp_replace(col("Telephone"), "[0-9]", "").alias("non_numeric_telephone"))
unique_chars_rdd = unique_chars_df.rdd.flatMap(lambda row: list(row.non_numeric_telephone)).distinct()
unique_chars = unique_chars_rdd.collect()

print("Unique characters other than numbers in Telephone column:")
print(unique_chars)

# COMMAND ----------

# Create a DataFrame showing the patterns
pattern_analysis = customers_df.withColumn(
    "PhonePattern", 
    extract_pattern_udf(col("Telephone"))
)

# Get a count of each unique pattern
pattern_counts = pattern_analysis.groupBy("PhonePattern").count().orderBy(desc("count"))

# Display all unique patterns ordered by frequency
print("All unique telephone number patterns:")
pattern_counts.show(50, truncate=False)  # Increased limit to show more patterns

# Get total count of unique patterns
unique_pattern_count = pattern_counts.count()
print(f"Total number of unique telephone patterns: {unique_pattern_count}")

# COMMAND ----------

# Cleaning telephone numbers by removing brackets, numbers after 'x' (including 'x'), dots, hyphens, and all whitespace (including spaces between digits)
# Apply the Cleaning Function
customers_df = clean_telephone_numbers(customers_df)

# Show a sample of cleaned telephone numbers
display(customers_df.select("Telephone").limit(5))

# COMMAND ----------

# Check for Extra Characters in the Telephone column other than '+'
extra_characters = find_extra_characters(customers_df, "Telephone")
if extra_characters:
    print(f"Extra characters in Telephone column: {extra_characters}")
else:
    print("There are no extra characters in Telephone column")

# COMMAND ----------

# Add country codes to telephone numbers
customers_df = add_country_codes(customers_df)

# Show a sample of formatted telephone numbers
display(customers_df.select("Country", "Telephone").limit(5))

# COMMAND ----------

# Verifying if all telephone numbers start with the '+' character
all_have_plus_prefix = check_all_telephones_start_with_plus(customers_df)
print(f"All Telephone Numbers start with '+': {all_have_plus_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of JobTitle Field with Consistent Default Values

# COMMAND ----------

# Replacing Null values in JobTitle column with 'Not Specified'
customers_df = customers_df.withColumn(
    "JobTitle", 
    when(col("JobTitle").isNull(), lit("Not Specified")).otherwise(col("JobTitle"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Text Fields through Consistent Formatting

# COMMAND ----------

# Standardize Column Names (Strip Whitespace, Convert to Lowercase and then Convert to Title Case)
columns_to_standardize = ["Name","City", "Country", "Gender", "JobTitle"]
customers_df = standardize_columns(customers_df, columns_to_standardize)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

print("Customers Data Cleaning Pipeline Complete...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer
# MAGIC Save the Cleaned and Enriched Customers Data to the Silver Layer.

# COMMAND ----------

# Save as Delta format in the Silver Layer
print(f"Writing {customers_df.count()} Customers to Silver Layer: {silver_customers_path}")

# Delete existing Delta files
dbutils.fs.rm(silver_customers_path, recurse=True)

# Apply Delta optimizations
customers_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(silver_customers_path)

print(f"Successfully wrote Customers Data to Silver Layer: {silver_customers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Silver Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the silver data
tmp_silver_customers_df = spark.read.format(file_format).load(silver_customers_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
silver_customers_df = spark.createDataFrame(tmp_silver_customers_df.rdd, customers_schema)

# Compare record counts
bronze_count = customers_df.count()
silver_count = silver_customers_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Silver record count: {silver_count}")
print(f"Records match: {bronze_count == silver_count}")

# Display sample from silver layer
print("Sample data from silver layer:")
display(silver_customers_df.limit(5))


# COMMAND ----------

silver_customers_df.printSchema()
