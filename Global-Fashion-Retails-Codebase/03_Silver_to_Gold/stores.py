# Databricks notebook source
# MAGIC %md
# MAGIC # Stores Data Transformation: Silver Layer to Gold Layer
# MAGIC
# MAGIC This Pipeline processes the Silver layer stores dataset and enriches it with analytical attributes:
# MAGIC
# MAGIC - Total Sales: Sum of all InvoiceTotalUSD by StoreID
# MAGIC - Average Monthly Sales: Average sales per month for each store
# MAGIC - Total Transactions: Count of distinct invoices (sales only) for each store
# MAGIC - Other Additional metrics such as ReturnRate, MonthsOfOperation, Sales per Employee
# MAGIC - Additional Store Performance Analysis

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
gold_invoice_fact_path = get_gold_path("invoice_fact")
gold_stores_path = get_gold_path("stores")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["gold"]

print(f"Processing Stores Data:")
print(f"- Stores Source: {silver_stores_path}")
print(f"- Invoice Fact Source: {gold_invoice_fact_path}")
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
print(f"Loaded {stores_df.count()} store records")

# Load Gold layer invoice_fact data
print(f"\nLoading Gold Invoice Fact Data...")
tmp_invoice_fact_df = spark.read.format(file_format).load(gold_invoice_fact_path)
invoice_fact_df = spark.createDataFrame(tmp_invoice_fact_df.rdd, invoice_fact_schema)
print(f"Loaded {invoice_fact_df.count()} invoice records")

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
# MAGIC ## Create Store Analytics Data

# COMMAND ----------

# Register tables as temporary views for SQL operations
stores_df.createOrReplaceTempView("stores")
invoice_fact_df.createOrReplaceTempView("invoice_fact")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using SQL for efficient transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total sales, transaction counts, and other metrics by store
# MAGIC CREATE OR REPLACE TEMPORARY VIEW store_sales_metrics AS
# MAGIC SELECT 
# MAGIC     f.StoreID,
# MAGIC     -- Total sales (USD) for sales transactions only
# MAGIC     SUM(CASE WHEN f.TransactionType = 'Sale' THEN f.InvoiceTotalUSD ELSE 0 END) AS TotalSales,
# MAGIC     
# MAGIC     -- Count of distinct sales invoices
# MAGIC     COUNT(DISTINCT CASE WHEN f.TransactionType = 'Sale' THEN f.InvoiceID END) AS TotalTransactions,
# MAGIC     
# MAGIC     -- Count of distinct return invoices
# MAGIC     COUNT(DISTINCT CASE WHEN f.TransactionType = 'Return' THEN f.InvoiceID END) AS TotalReturns,
# MAGIC     
# MAGIC     -- Min and Max dates to calculate operating period
# MAGIC     MIN(f.Date) AS FirstTransactionDate,
# MAGIC     MAX(f.Date) AS LastTransactionDate,
# MAGIC     
# MAGIC     -- Count of distinct months with transactions for monthly average calculation
# MAGIC     COUNT(DISTINCT DATE_FORMAT(f.Date, 'yyyy-MM')) AS MonthsWithTransactions
# MAGIC FROM 
# MAGIC     invoice_fact f
# MAGIC GROUP BY 
# MAGIC     f.StoreID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now join with the original stores data and calculate additional metrics
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gold_stores AS
# MAGIC SELECT 
# MAGIC     s.*,
# MAGIC     COALESCE(m.TotalSales, 0) AS TotalSales,
# MAGIC     COALESCE(m.TotalTransactions, 0) AS TotalTransactions,
# MAGIC     COALESCE(m.TotalReturns, 0) AS TotalReturns,
# MAGIC     
# MAGIC     -- Calculate average monthly sales
# MAGIC     -- If there are no months with transactions, default to 0
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(m.MonthsWithTransactions, 0) > 0 
# MAGIC         THEN CAST(COALESCE(m.TotalSales, 0) / m.MonthsWithTransactions AS DECIMAL(10,2))
# MAGIC         ELSE 0 
# MAGIC     END AS AverageMonthlyUSD,
# MAGIC     
# MAGIC     -- Calculate return rate (returns / total transactions)
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(m.TotalTransactions, 0) > 0 
# MAGIC         THEN CAST(COALESCE(m.TotalReturns, 0) * 100.0 / m.TotalTransactions AS DECIMAL(5,2))
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
# MAGIC     ) AS MonthsOfOperation,
# MAGIC     
# MAGIC     -- Calculate sales per employee
# MAGIC     CASE 
# MAGIC         WHEN s.NumberOfEmployees > 0 
# MAGIC         THEN CAST(COALESCE(m.TotalSales, 0) / s.NumberOfEmployees AS DECIMAL(10,2))
# MAGIC         ELSE 0 
# MAGIC     END AS SalesPerEmployee
# MAGIC FROM 
# MAGIC     stores s
# MAGIC LEFT JOIN 
# MAGIC     store_sales_metrics m ON s.StoreID = m.StoreID

# COMMAND ----------

# Convert SQL view to DataFrame
gold_stores_df = spark.sql("""
SELECT * FROM gold_stores
""")

# COMMAND ----------

# Record count for verification
gold_stores_count = gold_stores_df.count()

# Display sample enriched store data
print("Enriched Store Data Sample:")
display(gold_stores_df.limit(5))

# COMMAND ----------

display(gold_stores_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Store Performance Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create summary of store performance by country
# MAGIC SELECT 
# MAGIC     Country,
# MAGIC     COUNT(*) AS StoreCount,
# MAGIC     ROUND(SUM(TotalSales), 2) AS CountryTotalSales,
# MAGIC     ROUND(AVG(TotalSales), 2) AS AvgStoreRevenue,
# MAGIC     ROUND(AVG(AverageMonthlyUSD), 2) AS AvgMonthlyRevenue,
# MAGIC     ROUND(AVG(ReturnRate), 2) AS AvgReturnRate,
# MAGIC     ROUND(AVG(SalesPerEmployee), 2) AS AvgSalesPerEmployee
# MAGIC FROM 
# MAGIC     gold_stores
# MAGIC GROUP BY 
# MAGIC     Country
# MAGIC ORDER BY 
# MAGIC     CountryTotalSales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify top performing stores
# MAGIC SELECT 
# MAGIC     StoreID,
# MAGIC     StoreName,
# MAGIC     City,
# MAGIC     Country,
# MAGIC     TotalSales,
# MAGIC     AverageMonthlyUSD,
# MAGIC     TotalTransactions,
# MAGIC     NumberOfEmployees,
# MAGIC     SalesPerEmployee,
# MAGIC     ReturnRate
# MAGIC FROM 
# MAGIC     gold_stores
# MAGIC ORDER BY 
# MAGIC     TotalSales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer
# MAGIC Save the enriched Store Data to the Gold Layer.

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {gold_stores_count} store records to Gold Layer: {gold_stores_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_stores_path, recurse=True)

# Apply Delta optimizations
gold_stores_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_stores_path)

print(f"Successfully wrote Store Data to Gold Layer: {gold_stores_path}")

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
gold_stores_verify_df.select("TotalSales", "AverageMonthlyUSD", "TotalTransactions", "ReturnRate", "SalesPerEmployee").summary().show()

# COMMAND ----------

# Create a view for final reporting and visualization
gold_stores_verify_df.createOrReplaceTempView("gold_stores_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final store performance ranking
# MAGIC SELECT 
# MAGIC     StoreID,
# MAGIC     StoreName,
# MAGIC     City, 
# MAGIC     Country,
# MAGIC     TotalSales,
# MAGIC     AverageMonthlyUSD,
# MAGIC     TotalTransactions,
# MAGIC     ReturnRate,
# MAGIC     RANK() OVER (ORDER BY TotalSales DESC) AS SalesRank,
# MAGIC     RANK() OVER (ORDER BY AverageMonthlyUSD DESC) AS MonthlyRank,
# MAGIC     RANK() OVER (ORDER BY SalesPerEmployee DESC) AS EfficiencyRank,
# MAGIC     RANK() OVER (ORDER BY ReturnRate ASC) AS QualityRank
# MAGIC FROM 
# MAGIC     gold_stores_final
# MAGIC ORDER BY 
# MAGIC     SalesRank
