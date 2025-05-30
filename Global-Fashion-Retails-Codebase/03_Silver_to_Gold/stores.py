# Databricks notebook source
# MAGIC %md
# MAGIC # Stores Data Transformation: Silver Layer to Gold Layer
# MAGIC
# MAGIC This Pipeline processes the Silver layer Stores dataset and enriches it with analytical attributes and perform basic analysis:
# MAGIC - Calculating Total Sales, Total Transaction Counts, and Total Return Counts for Stores Data
# MAGIC - Calculating Average Monthly Sales, Return Rate, and Months of Operation for Stores Data
# MAGIC - Saving the Enriched Stores Data to the Gold Layer
# MAGIC - Analyzing Stores Performance by Country
# MAGIC - Analyzing Stores Performance Ranking
# MAGIC - Analyzing Underperforming Stores

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
gold_invoice_line_items_path = get_gold_path("invoice_line_items")
gold_stores_path = get_gold_path("stores")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["gold"]

print(f"Processing Stores Data:")
print(f"- Stores Source: {silver_stores_path}")
print(f"- Invoice Line Items Source: {gold_invoice_line_items_path}")
print(f"- Destination: {gold_stores_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Required Data

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, count, sum, avg, countDistinct, round, when, datediff, months_between
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# COMMAND ----------

# Load Silver layer stores data
print(f"Loading Silver Stores Data...")
tmp_stores_df = spark.read.format(file_format).load(silver_stores_path)
stores_df = spark.createDataFrame(tmp_stores_df.rdd, stores_schema)
stores_df.cache()
print(f"Loaded {stores_df.count()} store records")

# Load Gold layer invoice_line_items data
print(f"\nLoading Gold Invoice Line Items Data...")
tmp_invoice_line_items_df = spark.read.format(file_format).load(gold_invoice_line_items_path)
invoice_line_items_df = spark.createDataFrame(tmp_invoice_line_items_df.rdd, invoice_line_items_schema)
invoice_line_items_df.cache()
print(f"Loaded {invoice_line_items_df.count()} invoice records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display sample data
print("Stores Data Sample:")
display(stores_df.limit(5))

# COMMAND ----------

# Display schema
print("Stores Data Schema:")
stores_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculating Total Sales, Total Transaction Counts, and Total Return Counts for Stores Data

# COMMAND ----------

# Register tables as temporary views for SQL operations
stores_df.createOrReplaceTempView("stores")
invoice_line_items_df.createOrReplaceTempView("invoice_line_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total sales, transaction counts, and other metrics by store
# MAGIC CREATE OR REPLACE TEMPORARY VIEW store_sales_metrics AS
# MAGIC SELECT 
# MAGIC     f.StoreID,
# MAGIC     -- Total sales (USD) for sales transactions only
# MAGIC     SUM(CASE WHEN f.TransactionType = 'Sale' THEN f.LineTotalUSD ELSE 0 END) AS TotalSalesUSD,
# MAGIC     
# MAGIC     -- Count of distinct sales invoices
# MAGIC     COUNT(DISTINCT CASE WHEN f.TransactionType = 'Sale' THEN f.InvoiceID END) AS TotalTransactions,
# MAGIC     
# MAGIC     -- Count of distinct return invoices
# MAGIC     COUNT(DISTINCT CASE WHEN f.TransactionType = 'Return' THEN f.InvoiceID END) AS TotalReturns,
# MAGIC
# MAGIC     -- Total returns (USD) for returns transactions only
# MAGIC     SUM(CASE WHEN f.TransactionType = 'Return' THEN ABS(f.LineTotalUSD) ELSE 0 END) AS TotalReturnsUSD,
# MAGIC     
# MAGIC     -- Min and Max dates to calculate operating period
# MAGIC     MIN(f.Date) AS FirstTransactionDate,
# MAGIC     MAX(f.Date) AS LastTransactionDate,
# MAGIC     
# MAGIC     -- Count of distinct months with transactions for monthly average calculation
# MAGIC     COUNT(DISTINCT DATE_FORMAT(f.Date, 'yyyy-MM')) AS MonthsWithTransactions
# MAGIC FROM 
# MAGIC     invoice_line_items f
# MAGIC GROUP BY 
# MAGIC     f.StoreID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculating Average Monthly Sales, Return Rate, and Months of Operation for Stores Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now join with the original stores data and calculate additional metrics
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gold_stores AS
# MAGIC SELECT 
# MAGIC     s.*,
# MAGIC     COALESCE(m.TotalSalesUSD, 0) AS TotalSalesUSD,
# MAGIC     COALESCE(m.TotalTransactions, 0) AS TotalTransactions,
# MAGIC     COALESCE(m.TotalReturns, 0) AS TotalReturns,
# MAGIC     
# MAGIC     -- Calculate average monthly sales
# MAGIC     -- If there are no months with transactions, default to 0
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(m.MonthsWithTransactions, 0) > 0 
# MAGIC         THEN CAST(COALESCE(m.TotalSalesUSD, 0) / m.MonthsWithTransactions AS DECIMAL(10,4))
# MAGIC         ELSE 0 
# MAGIC     END AS AverageMonthlyUSD,
# MAGIC     
# MAGIC     -- Calculate return rate (total returns USD / total sales USD) * 100
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(m.TotalTransactions, 0) > 0 
# MAGIC         THEN CAST(COALESCE(m.TotalReturnsUSD, 0) * 100.0 / m.TotalSalesUSD AS DECIMAL(10,2))
# MAGIC         ELSE 0 
# MAGIC     END AS ReturnRate,
# MAGIC     
# MAGIC     -- Calculate months of operation
# MAGIC     CAST(
# MAGIC         CASE 
# MAGIC             WHEN m.FirstTransactionDate IS NOT NULL 
# MAGIC             THEN MONTHS_BETWEEN(m.LastTransactionDate, m.FirstTransactionDate) + 1
# MAGIC             ELSE 0 
# MAGIC         END AS DECIMAL(5,1)
# MAGIC     ) AS MonthsOfOperation
# MAGIC FROM 
# MAGIC     stores s
# MAGIC LEFT JOIN 
# MAGIC     store_sales_metrics m ON s.StoreID = m.StoreID

# COMMAND ----------

# Convert SQL view to DataFrame
gold_stores_df = spark.sql("""
SELECT * FROM gold_stores
""")
gold_stores_df.cache()

# COMMAND ----------

# Record count for verification
gold_stores_count = gold_stores_df.count()

# Display sample enriched store data
print("Enriched Stores Data Sample:")
display(gold_stores_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer
# MAGIC Save the enriched Stores Data to the Gold Layer.

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {gold_stores_count} Stores records to Gold Layer: {gold_stores_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_stores_path, recurse=True)

# Apply Delta optimizations
gold_stores_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_stores_path)

print(f"Successfully wrote {gold_stores_count} Stores Data to Gold Layer: {gold_stores_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Gold Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the Gold Data
tmp_gold_stores_df = spark.read.format(file_format).load(gold_stores_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
gold_stores_verify_df = spark.createDataFrame(tmp_gold_stores_df.rdd, gold_stores_schema)

# Compare record counts
gold_stores_verify_count = gold_stores_verify_df.count()
    
print(f"Gold stores record count: {gold_stores_verify_count}")
print(f"Records match: {gold_stores_count == gold_stores_verify_count}")

# COMMAND ----------

# Display final schema
print("Gold Stores Schema:")
gold_stores_verify_df.printSchema()

# COMMAND ----------

# Display a summary of store performance metrics
print("Summary Statistics for Store Performance Metrics:")
gold_stores_verify_df.select("TotalSalesUSD", "AverageMonthlyUSD", "TotalTransactions", "ReturnRate").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performing Data Analysis

# COMMAND ----------

# Create a view for final reporting and visualization
gold_stores_verify_df.createOrReplaceTempView("gold_stores_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyzing Stores Performance by Country

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create summary of store performance by country
# MAGIC SELECT 
# MAGIC     Country,
# MAGIC     COUNT(*) AS StoreCount,
# MAGIC     ROUND(SUM(TotalSalesUSD), 2) AS CountryTotalSales,
# MAGIC     ROUND(AVG(TotalSalesUSD), 2) AS AvgStoreRevenue,
# MAGIC     ROUND(AVG(AverageMonthlyUSD), 2) AS AvgMonthlyRevenue,
# MAGIC     ROUND(AVG(ReturnRate), 2) AS AvgReturnRate
# MAGIC FROM 
# MAGIC     gold_stores_final
# MAGIC GROUP BY 
# MAGIC     Country
# MAGIC ORDER BY 
# MAGIC     CountryTotalSales DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyzing Stores Performance Ranking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final store performance ranking
# MAGIC SELECT 
# MAGIC     StoreID,
# MAGIC     StoreName,
# MAGIC     City, 
# MAGIC     Country,
# MAGIC     TotalSalesUSD,
# MAGIC     AverageMonthlyUSD,
# MAGIC     TotalTransactions,
# MAGIC     ReturnRate,
# MAGIC     RANK() OVER (ORDER BY TotalSalesUSD DESC) AS SalesRank,
# MAGIC     RANK() OVER (ORDER BY AverageMonthlyUSD DESC) AS MonthlyRank,
# MAGIC     RANK() OVER (ORDER BY ReturnRate ASC) AS QualityRank
# MAGIC FROM 
# MAGIC     gold_stores_final
# MAGIC ORDER BY 
# MAGIC     SalesRank

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyzing Underperforming Stores

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify Under performing stores
# MAGIC SELECT 
# MAGIC     StoreID,
# MAGIC     StoreName,
# MAGIC     City,
# MAGIC     Country,
# MAGIC     TotalSalesUSD,
# MAGIC     AverageMonthlyUSD,
# MAGIC     TotalTransactions,
# MAGIC     NumberOfEmployees,
# MAGIC     ReturnRate
# MAGIC FROM 
# MAGIC     gold_stores_final
# MAGIC ORDER BY 
# MAGIC     TotalSalesUSD ASC
# MAGIC LIMIT 10
