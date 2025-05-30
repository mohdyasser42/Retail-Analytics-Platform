# Databricks notebook source
# MAGIC %md
# MAGIC # Common Utility Functions
# MAGIC
# MAGIC This Notebook Contains Shared Functions Used Across The Retail Analytics Processing Pipelines.

# COMMAND ----------

# Importing Necessary Libraries
from pyspark.sql.functions import col, trim, lower, regexp_replace, when, concat, lit, substring, initcap, regexp_extract, length
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import re

# COMMAND ----------

# Text Standardization: strip whitespace, convert to lowercase, and capitalize the first letter of each word
def standardize_column(dataframe, column_name):
    return dataframe.withColumn(
        column_name,
        initcap(lower(trim(col(column_name))))
    )

# Apply to multiple columns
def standardize_columns(dataframe, column_list):
    result_df = dataframe
    for column_name in column_list:
        result_df = standardize_column(result_df, column_name)
    return result_df

# COMMAND ----------

# Function to check if all rows have '@' in their Email column
def check_all_emails_valid(df):
    # Count total rows
    total_count = df.count()
    
    # Count valid emails (containing '@')
    valid_count = df.filter(col("Email").contains("@")).count()
    
    # If counts match, all emails are valid
    return total_count == valid_count

# COMMAND ----------

# Function to Extract Pattern from Phone Numbers
def extract_pattern(phone):
    if not phone:
        return "NULL"
    
    # Replace digits with 'N', letters with 'L', and keep special characters
    pattern = ""
    for char in phone:
        if char.isdigit():
            pattern += "N"
        elif char.isalpha():
            pattern += "L"
        else:
            pattern += char
            
    return pattern
# Create the UDF for this Function
extract_pattern_udf = udf(extract_pattern, StringType())

# COMMAND ----------

# This Function will create a cleaned version of the Telephone column in one operation
def clean_telephone_numbers(df):
    # This uses chained regexp_replace functions to:
    # 1. Remove brackets () [] {}
    # 2. Remove 'x' and any digits that follow it
    # 3. Remove dots and hyphens
    # 4. Remove all whitespace (including spaces between digits)
    # 5. Trim any leading/trailing whitespace that might remain
    
    cleaned_df = df.withColumn(
        "Telephone", 
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            col("Telephone"), 
                            "[\\(\\)\\[\\]\\{\\}]", ""  # Remove all types of brackets
                        ),
                        "x\\d*", ""  # Remove 'x' and any digits after it
                    ),
                    "[\\.-]", ""  # Remove dots and hyphens
                ),
                "\\s+", ""  # Remove all whitespace (including spaces between digits)
            )
        )
    )
    
    return cleaned_df

# COMMAND ----------

# Function to find extra characters in the Telephone column other than '+'
def find_extra_characters(df, column):
    pattern = re.compile(r'[^+\d]')
    extra_chars = df.select(column).rdd.flatMap(lambda x: pattern.findall(x[0])).distinct().collect()
    return extra_chars

# COMMAND ----------

# Function to add country codes to Telephone numbers
def add_country_codes(df):
    # Define country code mapping
    country_codes = {
        "United Kingdom": "44",
        "China": "86",
        "France": "33",
        "Germany": "49",
        "Spain": "34",
        "United States": "1",
        "Portugal": "351"
    }
    
    # Create a new column with properly formatted telephone numbers
    formatted_df = df.withColumn(
        "Telephone",
        # Check multiple conditions in order of precedence:
        
        # 1. Numbers that already start with '+' - keep as is
        when(
            col("Telephone").startswith("+"),
            col("Telephone")
        )
        
        # 2. Numbers that start with '0' followed by a valid country code
        # Instead of using isin with a collection, use explicit OR conditions
        .when(
            (regexp_extract(col("Telephone"), "^0(1)", 1) == "1") |
            (regexp_extract(col("Telephone"), "^0(33)", 1) == "33") |
            (regexp_extract(col("Telephone"), "^0(34)", 1) == "34") |
            (regexp_extract(col("Telephone"), "^0(44)", 1) == "44") |
            (regexp_extract(col("Telephone"), "^0(49)", 1) == "49") |
            (regexp_extract(col("Telephone"), "^0(86)", 1) == "86") |
            (regexp_extract(col("Telephone"), "^0(351)", 1) == "351"),
            # Replace the leading '0' with '+' and keep the rest of the number
            concat(
                lit("+"),
                substring(col("Telephone"), 2, length(col("Telephone")))
            )
        )
        
        # 3. Other numbers that need country code based on Country column
        .otherwise(
            # Add country code based on Country column and remove leading zeros
            concat(
                when(col("Country") == "United Kingdom", lit("+44"))
                .when(col("Country") == "China", lit("+86"))
                .when(col("Country") == "France", lit("+33"))
                .when(col("Country") == "Germany", lit("+49"))
                .when(col("Country") == "Spain", lit("+34"))
                .when(col("Country") == "United States", lit("+1"))
                .when(col("Country") == "Portugal", lit("+351"))
                .otherwise(lit("")),  # Empty string for unknown countries
                
                # Remove leading zeros patterns (0, 01, 00, 001) using regex
                regexp_replace(
                    col("Telephone"), 
                    "^(1|01|001|0{1,2})", ""  # Matches '0', '00', '01', or '001' at the start
                )
            )
        )
    )
    
    return formatted_df

# COMMAND ----------

# Function to check if all telephone numbers start with the '+' character, returning a single boolean result
def check_all_telephones_start_with_plus(df):
    # Count total rows
    total_count = df.count()
    
    # Count telephone numbers that start with '+'
    plus_prefix_count = df.filter(col("Telephone").startswith("+")).count()
    
    # Return True if all telephone numbers start with '+', False otherwise
    return total_count == plus_prefix_count

# COMMAND ----------

# Function to create and populate exchange rate table using freecurrencyapi
def fetch_exchange_rates(apikey, path, start_date="2023-01-01", end_date="2025-04-01"):
    """
    Fetch exchange rates from freecurrencyapi for all first-of-month dates 
    between start_date and end_date
    """
    try:
        # Import the libraries
        import time
        import freecurrencyapi
        from datetime import datetime, timedelta
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
        from dateutil.relativedelta import relativedelta
        
        # Initialize client with the API key
        api_key = apikey 
        client = freecurrencyapi.Client(api_key)
        
        # Define currencies
        base_currency = "USD"
        target_currencies = ["EUR", "GBP", "CNY"]
        
        # Generate all first-of-month dates from start_date to end_date
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Get the first day of the month for the start date
        current = datetime(start.year, start.month, 1)
        
        # Generate all first-of-month dates
        dates = []
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            # Move to first day of next month
            current = (current + relativedelta(months=1))
        
        # Prepare data structure for exchange rates
        rate_data = []
        
        # For each date, fetch exchange rates (respecting API rate limits)
        for date_str in dates:
            try:
                # Make API call with proper format based on your test result
                result = client.historical(date_str, base_currency, target_currencies)
                
                # Based on your API response format: {'data': {'2023-01-01': {'CNY': 6.897913, 'EUR': 0.934186, 'GBP': 0.826448}}}
                if result and 'data' in result and date_str in result['data']:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                    rates = result['data'][date_str]
                    
                    # Process rates for each currency
                    for currency, rate in rates.items():
                        # Store direct rate (USD to currency)
                        rate_data.append((date_obj, base_currency, currency, rate))
                        
                        # Store inverse rate (currency to USD)
                        if rate > 0:  # Avoid division by zero
                            inverse_rate = 1.0 / rate
                            rate_data.append((date_obj, currency, base_currency, inverse_rate))
                
                # Sleep to respect rate limit (10 requests per minute = 6 seconds per request to be safe)
                time.sleep(6)
                
            except Exception as e:
                print(f"Error fetching rates for {date_str}: {str(e)}")
                # Continue with next date even if one fails
                continue
        
        # schema for the exchange rates
        exchange_rate_schema = StructType([
            StructField("RateDate", DateType(), False),
            StructField("BaseCurrency", StringType(), False),
            StructField("TargetCurrency", StringType(), False),
            StructField("ExchangeRate", DoubleType(), False)
        ])
        
        # Create DataFrame from API data
        exchange_rates_df = spark.createDataFrame(rate_data, schema=exchange_rate_schema)

        # Save exchange rates to Delta
        exchange_rates_df.coalesce(1) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(path)
        
        return exchange_rates_df
        
    except ImportError:
        print("freecurrencyapi library not available, please check your environment")
        return None
