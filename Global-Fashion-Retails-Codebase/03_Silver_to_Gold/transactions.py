# Databricks notebook source
# MAGIC %md
# MAGIC # Transactions Data Transformation: Silver Layer to Gold Layer
# MAGIC
# MAGIC This Pipeline Enhances the Silver layer Transactions dataset and enriches it with analytical attributes:
# MAGIC
# MAGIC - Splits transactions into invoice_fact and invoice_line_items tables
# MAGIC - Invoice_fact contains Invoice 
# MAGIC - Converts currency values of InvoiceTotal column to USD using exchange rates
# MAGIC - Adding DiscountID to invoice_line_items by matching transaction date, category, subcategory and discount value

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
silver_stores_path = get_silver_path("stores")
silver_products_path = get_silver_path("products")
silver_transactions_path = get_silver_path("transactions")
silver_discounts_path = get_silver_path("discounts")
gold_invoice_fact_path = get_gold_path("invoice_fact")
gold_invoice_line_items_path = get_gold_path("invoice_line_items")
exchange_rates_path = get_gold_path("exchange_rates")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["gold"]

print(f"Processing Transactions Data:")
print(f"- Source: {silver_transactions_path}")
print(f"- Destination 1: {gold_invoice_fact_path}")
print(f"- Destination 2: {gold_invoice_line_items_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Layer Data

# COMMAND ----------

# Import Neccessary Libraries
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# COMMAND ----------

# Load Silver Layer Transactions Data
print("\nLoading Silver Transactions Data...")
tmp_transactions_df = spark.read.format(file_format).load(silver_transactions_path)
transactions_df = spark.createDataFrame(tmp_transactions_df.rdd, transactions_schema)
transactions_df.cache()
print(f"Loaded {transactions_df.count()} Transactions Records")

# Load Silver Layer Stores Data
print("\nLoading Silver Stores Data...")
tmp_stores_df = spark.read.format(file_format).load(silver_stores_path)
stores_df = spark.createDataFrame(tmp_stores_df.rdd, stores_schema)
stores_df.cache()
print(f"Loaded {stores_df.count()} Stores Records")

# Load Silver Layer Products Data
print("\nLoading Silver Products Data...")
tmp_products_df = spark.read.format(file_format).load(silver_products_path)
products_df = spark.createDataFrame(tmp_products_df.rdd, products_schema)
products_df.cache()
print(f"Loaded {products_df.count()} Products Records")

# Load Silver Layer Discounts Data
print("\nLoading Silver Discounts Data...")
tmp_discounts_df = spark.read.format(file_format).load(silver_discounts_path)
discounts_df = spark.createDataFrame(tmp_discounts_df.rdd, discounts_schema2)
discounts_df.cache()
print(f"Loaded {discounts_df.count()} Discounts Records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display sample data
print("Transactions Data Sample:")
display(transactions_df.limit(5))

# COMMAND ----------

# Display schema
print("Transactions Data Schema:")
transactions_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting Currency Values to USD using Exchange Rates

# COMMAND ----------

# Load the Exchange Rates Data
print("Loading Exchange Rates Data...")
# Inheriting API key for freecurrencyapi from config file
api_key = API_KEY
try:
    # Check if the exchange rates table exists
    tmp_exchange_rates = spark.read.format(file_format).load(exchange_rates_path)
    exchange_rates_df = spark.createDataFrame(tmp_exchange_rates.rdd, exchange_rates_schema)
    display(exchange_rates_df.limit(10))
except:
    # Call function to generate exchange rates table
    exchange_rates_df = fetch_exchange_rates(api_key,exchange_rates_path)
    display(exchange_rates_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Splitting Transactions Data into Two Different Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Invoice_fact Table

# COMMAND ----------

# Create invoice_fact table with aggregations at invoice level

# First, Extract Date and Time from Timestamp
invoice_fact = transactions_df.withColumn("Date", F.to_date("Timestamp")) \
    .withColumn("Time", F.date_format("Timestamp", "HH:mm:ss")) \
    .withColumn("FirstOfMonth", F.trunc("Timestamp", "month"))  # First day of month for exchange rate join


# COMMAND ----------

# Group by invoice to get total quantity and other invoice-level attributes
invoice_fact = invoice_fact.groupBy("InvoiceID", "CustomerID", "Date", "Time", "FirstOfMonth",
                                     "StoreID", "EmployeeID", "Currency", "CurrencySymbol", 
                                     "TransactionType", "PaymentMethod", "InvoiceTotal") \
    .agg(F.sum("Quantity").alias("TotalQuantity"))


# COMMAND ----------

# Selecting Country and City Columns from Stores Data
stores_df1 = stores_df.select("StoreID", "Country", "City").withColumnRenamed("Country", "StoreCountry").withColumnRenamed("City", "StoreCity")

# Join with store data to add StoreCountry and StoreCity columns
invoice_fact = invoice_fact.join(stores_df1, on="StoreID", how="left")

# COMMAND ----------

# Create a view for easier SQL-based transformations
exchange_rates_df.createOrReplaceTempView("exchange_rates")
invoice_fact.createOrReplaceTempView("invoice_fact_temp")

# COMMAND ----------

# Use SQL for the currency conversion logic
invoice_fact_with_usd = spark.sql("""
    SELECT 
        i.*,
        CAST(
            CASE 
                WHEN i.Currency = 'USD' THEN i.InvoiceTotal
                ELSE i.InvoiceTotal * COALESCE(e.ExchangeRate, 1.0)
            END AS DECIMAL(10,4)
        ) AS InvoiceTotalUSD
    FROM 
        invoice_fact_temp i
    LEFT JOIN 
        exchange_rates e ON i.FirstOfMonth = e.RateDate 
                        AND i.Currency = e.BaseCurrency 
                        AND e.TargetCurrency = 'USD'
""")

# COMMAND ----------

# Select and order columns for final invoice_fact table
invoice_fact = invoice_fact_with_usd.select(
    "InvoiceID", "CustomerID", "Date", "Time","StoreID", "StoreCountry",
    "StoreCity", "EmployeeID", "TotalQuantity", "Currency", "CurrencySymbol", 
    "TransactionType", "PaymentMethod", "InvoiceTotal", "InvoiceTotalUSD"
)
invoice_fact_count = invoice_fact.count()

# COMMAND ----------

# Display sample data of non-USD transactions
display(invoice_fact.filter(invoice_fact.StoreCountry == 'China').limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Invoice_line_items Table with Discount ID Tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Extract product category information

# COMMAND ----------

# Extract Date and Time from Timestamp
invoice_line_items = transactions_df.withColumn("Date", F.to_date("Timestamp")) \
    .withColumn("Time", F.date_format("Timestamp", "HH:mm:ss")) \
    .withColumn("FirstOfMonth", F.trunc("Timestamp", "month"))  # First day of month for exchange rate join

# COMMAND ----------

# Selecting Country and City Columns from Stores Data
stores_df2 = stores_df.select("StoreID", "Country", "City")

# Selecting Category and SubCategory columns from Product Data
products_df1 = products_df.select("ProductID", "Category", "SubCategory")

# COMMAND ----------

# Create base invoice_line_items table
invoice_line_items = invoice_line_items.withColumn("Date", F.to_date("Timestamp")) \
    .select(
    "InvoiceID", "CustomerID", "Line", "ProductID", "Size", "Color", 
    "UnitPrice", "Quantity", "Discount","Date", "FirstOfMonth",
    "StoreID", "SKU", "LineTotal", "TransactionType", "Currency"
)

# COMMAND ----------

# Join with product info to get Category and SubCategory
invoice_line_items = invoice_line_items.join(products_df1, on="ProductID", how="left")

# Join with store info to get Country and City
invoice_line_items = invoice_line_items.join(stores_df2, on="StoreID", how="left")

# COMMAND ----------

invoice_line_items.cache().count()
display(invoice_line_items.limit(5))

# COMMAND ----------

# Register tables as temporary views for SQL operations
exchange_rates_df.createOrReplaceTempView("exchange_rates")
invoice_line_items.createOrReplaceTempView("invoice_items_temp")

# COMMAND ----------

# Use SQL for the currency conversion logic
invoice_items_with_usd = spark.sql("""
    SELECT 
        i.*,
        CAST(
            CASE 
                WHEN i.Currency = 'USD' THEN i.LineTotal
                ELSE i.LineTotal * COALESCE(e.ExchangeRate, 1.0)
            END AS DECIMAL(10,4)
        ) AS LineTotalUSD
    FROM 
        invoice_items_temp i
    LEFT JOIN 
        exchange_rates e ON i.FirstOfMonth = e.RateDate 
                        AND i.Currency = e.BaseCurrency 
                        AND e.TargetCurrency = 'USD'
""")

# COMMAND ----------

invoice_line_items = invoice_items_with_usd.select("InvoiceID", "CustomerID", "Line", "ProductID", "Size", "Color", 
    "UnitPrice", "Quantity", "Discount","Date", "FirstOfMonth", "StoreID", "SKU", "LineTotal", "TransactionType", "Currency", "LineTotalUSD", "Category", "SubCategory", "Country", "City")


# COMMAND ----------

invoice_line_items.cache().count()
display(invoice_line_items.filter(invoice_line_items['Country'] == 'China').limit(5))

# COMMAND ----------

# Register tables as temporary views for SQL operations
discounts_df.createOrReplaceTempView("discounts")
invoice_line_items.createOrReplaceTempView("invoice_line_items_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view to find matching discounts for each transaction
# MAGIC -- IMPORTANT: This only assigns DiscountID for sales transactions with non-zero discounts
# MAGIC -- All other transactions will keep their original data but DiscountID will be NULL
# MAGIC CREATE OR REPLACE TEMPORARY VIEW transaction_discounts AS
# MAGIC SELECT 
# MAGIC     t.InvoiceID,
# MAGIC     t.Line,
# MAGIC     d.DiscountID
# MAGIC FROM 
# MAGIC     invoice_line_items_temp t
# MAGIC LEFT JOIN 
# MAGIC     discounts d
# MAGIC ON 
# MAGIC     -- Match on discount value
# MAGIC     t.Discount = d.Discount
# MAGIC     -- Check if transaction date is within the discount period
# MAGIC     AND t.Date >= d.StartDate
# MAGIC     AND t.Date <= d.EndDate
# MAGIC     -- Match on Category (either specific category or 'All')
# MAGIC     AND (d.Category = t.Category OR d.Category = 'All')
# MAGIC     -- Match on SubCategory (either specific subcategory or 'All')
# MAGIC     AND (d.SubCategory = t.SubCategory OR d.SubCategory = 'All')
# MAGIC WHERE
# MAGIC     -- Only perform this matching for sales transactions with discounts
# MAGIC     t.TransactionType = 'Sale'
# MAGIC     AND t.Discount > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now join the original data with the discount mapping
# MAGIC -- This preserves ALL original transactions while adding DiscountID where appropriate
# MAGIC CREATE OR REPLACE TEMPORARY VIEW invoice_line_items_with_discount AS
# MAGIC SELECT 
# MAGIC     t.InvoiceID,
# MAGIC     t.Line,
# MAGIC     t.CustomerID,
# MAGIC     t.ProductID,
# MAGIC     t.Size,
# MAGIC     t.Color,
# MAGIC     t.UnitPrice,
# MAGIC     t.Quantity,
# MAGIC     t.Discount,
# MAGIC     t.SKU,
# MAGIC     t.LineTotal,
# MAGIC     t.Date,
# MAGIC     t.TransactionType,
# MAGIC     t.Category,
# MAGIC     t.SubCategory,
# MAGIC     t.StoreID,
# MAGIC     t.Currency,
# MAGIC     t.Country,
# MAGIC     t.City,
# MAGIC     t.LineTotalUSD,
# MAGIC     d.DiscountID
# MAGIC FROM 
# MAGIC     invoice_line_items_temp t
# MAGIC LEFT JOIN 
# MAGIC     transaction_discounts d
# MAGIC ON 
# MAGIC     t.InvoiceID = d.InvoiceID
# MAGIC     AND t.Line = d.Line

# COMMAND ----------

# Convert the SQL view back to a DataFrame
invoice_line_items_with_discount = spark.sql("""
SELECT * FROM invoice_line_items_with_discount
""")

# COMMAND ----------

# Create surrogate keys for line items
window_spec = Window.orderBy("InvoiceID", "Line")
invoice_line_items_final = invoice_line_items_with_discount.withColumn("LineItemID", F.row_number().over(window_spec))

# COMMAND ----------

# Reorder columns with surrogate key first
invoice_line_items = invoice_line_items_final.select(
    "LineItemID", "InvoiceID", "Line", "CustomerID", "ProductID",
    "Category", "SubCategory", "StoreID", "Country", "City", "Size", "Color", "UnitPrice", "Quantity", 
    "Discount", "DiscountID", "SKU", "Date", "TransactionType", "LineTotal", "LineTotalUSD"
)
invoice_line_items.cache()
invoice_line_items_count = invoice_line_items.count()

# COMMAND ----------

# Display sample of discounted transactions
display(invoice_line_items.filter(col("Discount") > 0).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Contribution Margin of each Transaction Line

# COMMAND ----------

# Register tables as temporary views for SQL operations
invoice_line_items.createOrReplaceTempView("invoice_line_items")
products_df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the products table structure to confirm ProductionCost column
# MAGIC DESCRIBE products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate Contribution Margin for each line item
# MAGIC CREATE OR REPLACE TEMPORARY VIEW invoice_line_items_with_margin AS
# MAGIC SELECT 
# MAGIC     i.LineItemID,
# MAGIC     i.InvoiceID,
# MAGIC     i.Line,
# MAGIC     i.CustomerID,
# MAGIC     i.ProductID,
# MAGIC     i.Category,
# MAGIC     i.SubCategory,
# MAGIC     i.StoreID,
# MAGIC     i.Country,
# MAGIC     i.City,
# MAGIC     i.Size,
# MAGIC     i.Color,
# MAGIC     i.UnitPrice,
# MAGIC     i.Quantity,
# MAGIC     i.Discount,
# MAGIC     i.DiscountID,
# MAGIC     i.SKU,
# MAGIC     i.Date,
# MAGIC     i.TransactionType,
# MAGIC     i.LineTotal,
# MAGIC     i.LineTotalUSD,
# MAGIC     p.ProductionCost,
# MAGIC     
# MAGIC     -- Calculate Total Production Cost (Production Cost × Quantity)
# MAGIC     CAST(p.ProductionCost * i.Quantity AS DECIMAL(10,4)) AS TotalProductionCost,
# MAGIC     
# MAGIC     -- Calculate Contribution Margin (LineTotal - TotalProductionCost)
# MAGIC     -- Only for Sale transactions, for returns we'll keep it as NULL
# MAGIC     CASE 
# MAGIC         WHEN i.TransactionType = 'Sale' THEN 
# MAGIC             CAST(i.LineTotalUSD - (p.ProductionCost * i.Quantity) AS DECIMAL(10,4))
# MAGIC         ELSE NULL
# MAGIC     END AS ContributionMargin,
# MAGIC     
# MAGIC     -- Calculate Contribution Margin Percentage (Margin / Revenue × 100)
# MAGIC     -- Only for Sale transactions with positive LineTotalUSD
# MAGIC     CASE 
# MAGIC         WHEN i.TransactionType = 'Sale' AND i.LineTotalUSD > 0 THEN 
# MAGIC             CAST(((i.LineTotalUSD - (p.ProductionCost * i.Quantity)) / i.LineTotalUSD) * 100 AS DECIMAL(10,2))
# MAGIC         ELSE NULL
# MAGIC     END AS ContributionMarginPercentage
# MAGIC FROM 
# MAGIC     invoice_line_items i
# MAGIC LEFT JOIN 
# MAGIC     products p ON i.ProductID = p.ProductID

# COMMAND ----------

# Convert SQL view to DataFrame
invoice_line_items = spark.sql("""
SELECT * FROM invoice_line_items_with_margin
""")
invoice_line_items.cache()
invoice_line_items_count = invoice_line_items.count()

# COMMAND ----------

# Display sample with contribution margin data
print("Invoice Line Items with Contribution Margin:")
display(invoice_line_items.filter("TransactionType = 'Sale'").limit(10))

# COMMAND ----------

print("Transactions Data Transformation Pipeline Complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer
# MAGIC Save the Invoice_fact and Invoice_line_items Data to the Gold Layer.

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {invoice_fact_count} transactions to gold Layer: {gold_invoice_fact_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_invoice_fact_path, recurse=True)

# Apply Delta optimizations
invoice_fact.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_invoice_fact_path)

print(f"Successfully wrote Invoice_fact Data to Gold Layer: {gold_invoice_fact_path}")

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {invoice_line_items_count} transactions to Gold Layer: {gold_invoice_line_items_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_invoice_line_items_path, recurse=True)

# Apply Delta optimizations
invoice_line_items.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_invoice_line_items_path)

print(f"Successfully wrote Invoice_line_items Data to Gold Layer: {gold_invoice_line_items_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Gold Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the Gold Data
tmp_gold_invoice_fact_df = spark.read.format(file_format).load(gold_invoice_fact_path)
tmp_gold_invoice_line_items_df = spark.read.format(file_format).load(gold_invoice_line_items_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
gold_invoice_fact_df = spark.createDataFrame(tmp_gold_invoice_fact_df.rdd, invoice_fact_schema)
gold_invoice_line_items_df = spark.createDataFrame(tmp_gold_invoice_line_items_df.rdd, invoice_line_items_schema)

# Cache the DataFrames
gold_invoice_fact_df.cache()
gold_invoice_line_items_df.cache()

# Compare record counts
gold_invoice_fact_count = gold_invoice_fact_df.count()
gold_invoice_line_items_count = gold_invoice_line_items_df.count()

print(f"Gold invoice_fact record count: {gold_invoice_fact_count}")
print(f"Gold invoice_line_items record count: {gold_invoice_line_items_count}")

# COMMAND ----------

# Check record counts
print(f"Records match 1: {invoice_fact_count == gold_invoice_fact_count}")
print(f"Records match 2: {invoice_line_items_count == gold_invoice_line_items_count}")

# COMMAND ----------

# Check schema of the gold invoice_fact table
print("Gold Invoice Fact Schema:")
gold_invoice_fact_df.printSchema()

# COMMAND ----------

# Check schema of the gold invoice_line_items table with DiscountID
print("Gold Invoice Line Items Schema:")
gold_invoice_line_items_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

# Get Gold Discounts Data Path
gold_discounts_path = get_gold_path("discounts")

# Read the Gold Discounts Data
tmp_gold_discounts_df = spark.read.format(file_format).load(gold_discounts_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
gold_discounts_df = spark.createDataFrame(tmp_gold_discounts_df.rdd, discounts_schema2)

# COMMAND ----------

# Register Tables as Temporary Views for SQL Operations
gold_invoice_line_items_df.createOrReplaceTempView("final_invoice_line_items")
gold_discounts_df.createOrReplaceTempView("discounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Records with and without Discounts

# COMMAND ----------

# Display count of records with and without discounts
print("Transactions Summary:")
summary_df = spark.sql("""
SELECT
    TransactionType,
    CASE 
        WHEN Discount > 0 THEN 'Discounted' 
        ELSE 'Regular Price'
    END AS PriceType,
    CASE 
        WHEN DiscountID IS NOT NULL THEN 'With DiscountID' 
        WHEN Discount > 0 AND DiscountID IS NULL THEN 'Missing DiscountID'
        ELSE 'No DiscountID Required'
    END AS DiscountIDStatus,
    COUNT(*) AS RecordCount
FROM
    final_invoice_line_items
GROUP BY
    TransactionType,
    CASE 
        WHEN Discount > 0 THEN 'Discounted' 
        ELSE 'Regular Price'
    END,
    CASE 
        WHEN DiscountID IS NOT NULL THEN 'With DiscountID' 
        WHEN Discount > 0 AND DiscountID IS NULL THEN 'Missing DiscountID'
        ELSE 'No DiscountID Required'
    END
ORDER BY
    TransactionType,
    PriceType,
    DiscountIDStatus
""")
display(summary_df)

# COMMAND ----------

# Create a summary of discount usage to validate
print("Discount Usage Summary:")
spark.sql("""
SELECT 
    d.DiscountID,
    d.StartDate,
    d.EndDate,
    d.Discount,
    d.Category,
    d.SubCategory,
    COUNT(t.InvoiceID) AS TransactionCount,
    SUM(t.LineTotalUSD) AS TotalSales
FROM 
    discounts d
LEFT JOIN 
    final_invoice_line_items t ON d.DiscountID = t.DiscountID
GROUP BY 
    d.DiscountID, d.StartDate, d.EndDate, d.Discount, d.Category, d.SubCategory
ORDER BY 
    TransactionCount DESC
""").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Overall Margin Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate overall margin metrics
# MAGIC SELECT 
# MAGIC     'All Sales' AS Category,
# MAGIC     COUNT(*) AS LineItemCount,
# MAGIC     ROUND(SUM(LineTotalUSD), 2) AS TotalRevenue,
# MAGIC     ROUND(SUM(TotalProductionCost), 2) AS TotalCost,
# MAGIC     ROUND(SUM(ContributionMargin), 2) AS TotalMargin,
# MAGIC     ROUND((SUM(ContributionMargin) / SUM(LineTotalUSD)) * 100, 2) AS OverallMarginPercentage
# MAGIC FROM 
# MAGIC     final_invoice_line_items
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'
# MAGIC     AND LineTotalUSD > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Contribution Margin by Product Category

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze contribution margin by product category
# MAGIC SELECT 
# MAGIC     Category,
# MAGIC     COUNT(*) AS LineItemCount,
# MAGIC     ROUND(SUM(LineTotalUSD), 2) AS TotalRevenue,
# MAGIC     ROUND(SUM(TotalProductionCost), 2) AS TotalCost,
# MAGIC     ROUND(SUM(ContributionMargin), 2) AS TotalMargin,
# MAGIC     ROUND((SUM(ContributionMargin) / SUM(LineTotalUSD)) * 100, 2) AS CategoryMarginPercentage
# MAGIC FROM 
# MAGIC     final_invoice_line_items
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'
# MAGIC     AND LineTotalUSD > 0
# MAGIC GROUP BY 
# MAGIC     Category
# MAGIC ORDER BY 
# MAGIC     TotalMargin DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze contribution margin by discount usage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze contribution margin by discount usage
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN DiscountID IS NOT NULL THEN 'Discounted'
# MAGIC         ELSE 'Regular Price'
# MAGIC     END AS PriceType,
# MAGIC     COUNT(*) AS LineItemCount,
# MAGIC     ROUND(SUM(LineTotalUSD), 2) AS TotalRevenue,
# MAGIC     ROUND(SUM(TotalProductionCost), 2) AS TotalCost,
# MAGIC     ROUND(SUM(ContributionMargin), 2) AS TotalMargin,
# MAGIC     ROUND((SUM(ContributionMargin) / SUM(LineTotalUSD)) * 100, 2) AS MarginPercentage
# MAGIC FROM 
# MAGIC     final_invoice_line_items
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'
# MAGIC     AND LineTotalUSD > 0
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN DiscountID IS NOT NULL THEN 'Discounted'
# MAGIC         ELSE 'Regular Price'
# MAGIC     END
# MAGIC ORDER BY 
# MAGIC     MarginPercentage DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Distribution of Contribution Margin Percentages

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify distribution of contribution margin percentages
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN ContributionMarginPercentage < 0 THEN 'Negative Margin'
# MAGIC         WHEN ContributionMarginPercentage < 20 THEN 'Low (0-20%)'
# MAGIC         WHEN ContributionMarginPercentage < 40 THEN 'Medium (20-40%)'
# MAGIC         WHEN ContributionMarginPercentage < 60 THEN 'Good (40-60%)'
# MAGIC         WHEN ContributionMarginPercentage < 80 THEN 'High (60-80%)'
# MAGIC         ELSE 'Very High (80%+)'
# MAGIC     END AS MarginBucket,
# MAGIC     COUNT(*) AS LineItemCount,
# MAGIC     ROUND(SUM(LineTotalUSD), 2) AS TotalRevenue,
# MAGIC     ROUND(AVG(ContributionMarginPercentage), 2) AS AvgMarginPercentage
# MAGIC FROM 
# MAGIC     final_invoice_line_items
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'
# MAGIC     AND LineTotalUSD > 0
# MAGIC     AND ContributionMarginPercentage IS NOT NULL
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN ContributionMarginPercentage < 0 THEN 'Negative Margin'
# MAGIC         WHEN ContributionMarginPercentage < 20 THEN 'Low (0-20%)'
# MAGIC         WHEN ContributionMarginPercentage < 40 THEN 'Medium (20-40%)'
# MAGIC         WHEN ContributionMarginPercentage < 60 THEN 'Good (40-60%)'
# MAGIC         WHEN ContributionMarginPercentage < 80 THEN 'High (60-80%)'
# MAGIC         ELSE 'Very High (80%+)'
# MAGIC     END
# MAGIC ORDER BY 
# MAGIC     CASE 
# MAGIC         WHEN MarginBucket = 'Negative Margin' THEN 1
# MAGIC         WHEN MarginBucket = 'Low (0-20%)' THEN 2
# MAGIC         WHEN MarginBucket = 'Medium (20-40%)' THEN 3
# MAGIC         WHEN MarginBucket = 'Good (40-60%)' THEN 4
# MAGIC         WHEN MarginBucket = 'High (60-80%)' THEN 5
# MAGIC         ELSE 6
# MAGIC     END
