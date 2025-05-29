# Databricks notebook source
# MAGIC %md
# MAGIC # Customers Data Transformation: Silver Layer to Gold Layer
# MAGIC
# MAGIC This Pipeline processes the Silver layer customers dataset and enriches it with analytical attributes:
# MAGIC
# MAGIC - Age: Calculated from DateOfBirth
# MAGIC - AgeGroup: Categorization of customers into age segments
# MAGIC - IsParent: Boolean indicating if customer purchased children's products above threshold
# MAGIC - Total Spending: Sum of all purchases in USD by the customer
# MAGIC - No of Invoices: Count of distinct invoices associated with the customer
# MAGIC - Average Spending: Average amount spent per invoice by the customer

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
silver_customers_path = get_silver_path("customers")
gold_invoice_line_items_path = get_gold_path("invoice_line_items")
gold_customers_path = get_gold_path("customers")
gold_segment_metrics_path = get_gold_path("segment_metrics")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["gold"]
parent_threshold = 2  # Number of children's product purchases to qualify as parent

print(f"Processing Customers Data:")
print(f"- Customers Source: {silver_customers_path}")
print(f"- Destination: {gold_customers_path}")
print(f"- Parent Threshold: {parent_threshold} children's product purchases")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Required Data

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, count, when, lit, sum, avg, countDistinct, datediff, current_date, year, to_date, floor
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, IntegerType
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Load Silver layer customers data
print(f"Loading Silver Customers Data...")
tmp_customers_df = spark.read.format(file_format).load(silver_customers_path)
customers_df = spark.createDataFrame(tmp_customers_df.rdd, customers_schema)
customers_df.cache()
print(f"Loaded {customers_df.count()} customer records")

# Load Gold layer invoice_line_items data
print(f"\nLoading Gold Invoice Line Items Data...")
tmp_invoice_line_items_df = spark.read.format(file_format).load(gold_invoice_line_items_path)
invoice_line_items_df = spark.createDataFrame(tmp_invoice_line_items_df.rdd, invoice_line_items_schema)
invoice_line_items_df.cache()
print(f"Loaded {invoice_line_items_df.count()} invoice records")

# COMMAND ----------

tmp_customers_df = spark.read.format(file_format).load(silver_customers_path)
customers_df = spark.createDataFrame(tmp_customers_df.rdd, customers_schema)
customers_df.cache()
print(f"Loaded {customers_df.count()} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration and Profiling

# COMMAND ----------

# Display sample data
print("Customers Data Sample:")
display(customers_df.limit(5))

# COMMAND ----------

# Display schema
print("Customers Data Schema:")
customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Customer Age and Age Group

# COMMAND ----------

# Check for NULL dates
null_dob_count = customers_df.filter(col("DateOfBirth").isNull()).count()
print(f"Customers with NULL DateOfBirth: {null_dob_count}")

# COMMAND ----------

# Find max date from invoice_line_items
max_date = invoice_line_items_df.agg({"date": "max"}).collect()[0][0]
print(f"Max date from invoice_line_items: {max_date}")

# COMMAND ----------

last_date = (max_date.replace(day=1) + relativedelta(months=1))
print(f"First date of the next month of the last Transaction: {last_date}")

# COMMAND ----------

# Convert last_date to appropriate date format
last_date_str = last_date.strftime("%Y-%m-%d")
print(f"Formatted last_date: {last_date_str}")

# COMMAND ----------

# Add age calculation to customers dataframe
customers_df_with_age = customers_df.withColumn(
    "Age", 
    floor(datediff(lit(last_date_str), to_date("DateOfBirth", "yyyy-MM-dd")) / 365.25)
)

display(customers_df_with_age.limit(5))

# COMMAND ----------

# Handle potentially invalid ages (negative or extremely high)
customers_df_with_age = customers_df_with_age.withColumn(
    "Age",
    when((col("Age") < 0) | (col("Age") > 120), None).otherwise(col("Age"))
)

# COMMAND ----------

# Add age group categorization
customers_df = customers_df_with_age.withColumn(
    "AgeGroup",
    when(col("Age").isNull(), "Unknown")
    .when(col("Age") < 18, "Under 18")
    .when(col("Age") < 25, "18-24")
    .when(col("Age") < 35, "25-34")
    .when(col("Age") < 45, "35-44")
    .when(col("Age") < 55, "45-54")
    .when(col("Age") < 65, "55-64")
    .otherwise("65+")
)

# COMMAND ----------

# Display age distribution
print("Customer Age Distribution:")
customers_df.cache()
display(customers_df.groupBy("AgeGroup").count().orderBy("AgeGroup"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Customer Analytics Data

# COMMAND ----------

# Register tables as temporary views for SQL operations
customers_df.createOrReplaceTempView("customers")
invoice_line_items_df.createOrReplaceTempView("invoice_line_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using SQL for efficient transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view that identifies customers who are parents based on children's product purchases
# MAGIC     CREATE OR REPLACE TEMPORARY VIEW parent_customers AS
# MAGIC     SELECT 
# MAGIC         CustomerID,
# MAGIC         COUNT(*) as children_product_purchases
# MAGIC     FROM 
# MAGIC         invoice_line_items
# MAGIC     WHERE 
# MAGIC         Category = 'Children' AND TransactionType = 'Sale'
# MAGIC     GROUP BY 
# MAGIC         CustomerID
# MAGIC     HAVING 
# MAGIC         COUNT(*) >= 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view with spending metrics per customer
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_spending AS
# MAGIC SELECT 
# MAGIC     CustomerID,
# MAGIC     SUM(LineTotalUSD) AS TotalSpending,
# MAGIC     COUNT(DISTINCT InvoiceID) AS NoOfInvoices,
# MAGIC     CAST(SUM(LineTotalUSD) / COUNT(DISTINCT InvoiceID) AS DECIMAL(10,4)) AS AverageSpending
# MAGIC FROM 
# MAGIC     invoice_line_items
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'  -- Only include actual sales invoices, not returns
# MAGIC GROUP BY 
# MAGIC     CustomerID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the gold layer customers with analytics attributes
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gold_customers AS
# MAGIC SELECT 
# MAGIC     c.*,
# MAGIC     CASE WHEN p.CustomerID IS NOT NULL THEN true ELSE false END AS IsParent,
# MAGIC     COALESCE(s.TotalSpending, 0) AS TotalSpending,
# MAGIC     COALESCE(s.NoOfInvoices, 0) AS NoOfInvoices,
# MAGIC     COALESCE(s.AverageSpending, 0) AS AverageSpending
# MAGIC FROM 
# MAGIC     customers c
# MAGIC LEFT JOIN 
# MAGIC     parent_customers p ON c.CustomerID = p.CustomerID
# MAGIC LEFT JOIN 
# MAGIC     customer_spending s ON c.CustomerID = s.CustomerID

# COMMAND ----------

# Convert SQL view to DataFrame
gold_customers_df = spark.sql("""
SELECT * FROM gold_customers
""")
gold_customers_df.cache()

# COMMAND ----------

# Record count for verification
gold_customers_count = gold_customers_df.count()
# Display sample enriched customer data
print("Enriched Customer Data Sample:")
display(gold_customers_df.limit(5))

# COMMAND ----------

# Count distinct JobTitle
distinct_job_title_count = gold_customers_df.select("JobTitle").distinct().count()
distinct_job_title_count

# COMMAND ----------

# Describe Customer Data
gold_customers_df.select("TotalSpending","NoOfInvoices","AverageSpending").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFM Analysis (Recency, Frequency, Monetary) for Customer Segmentation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Recency (days since last purchase)

# COMMAND ----------

# Register tables as temporary views for SQL operations
gold_customers_df.createOrReplaceTempView("gold_customers")
invoice_line_items_df.createOrReplaceTempView("invoice_line_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view with the last purchase date for each customer
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_last_purchase AS
# MAGIC SELECT 
# MAGIC     CustomerID,
# MAGIC     MAX(Date) AS LastPurchaseDate
# MAGIC FROM 
# MAGIC     invoice_line_items
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'
# MAGIC GROUP BY 
# MAGIC     CustomerID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate RFM metrics
# MAGIC -- Using the value of last_date_str to calculate the recency metric
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_rfm AS
# MAGIC SELECT 
# MAGIC     c.CustomerID,
# MAGIC     DATEDIFF('2025-04-01', COALESCE(lp.LastPurchaseDate, '2025-04-01')) AS Recency,
# MAGIC     c.NoOfInvoices AS Frequency,
# MAGIC     c.TotalSpending AS Monetary
# MAGIC FROM 
# MAGIC     gold_customers c
# MAGIC LEFT JOIN 
# MAGIC     customer_last_purchase lp ON c.CustomerID = lp.CustomerID

# COMMAND ----------

# MAGIC %md
# MAGIC ### Score RFM Dimensions and Create RFM Segments

# COMMAND ----------

# Create quintiles for RFM metrics
rfm_df = spark.sql("""
SELECT 
    *,
    NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,  -- Lowest recency (most recent) gets 5
    NTILE(5) OVER (ORDER BY Frequency) AS F_Score,     -- Highest frequency gets 5
    NTILE(5) OVER (ORDER BY Monetary) AS M_Score       -- Highest monetary gets 5
FROM 
    customer_rfm
""")

# Add RFM_Score
rfm_df = rfm_df.withColumn("RFM_Score", 
                           F.concat(
                               F.col("R_Score").cast("string"), 
                               F.col("F_Score").cast("string"), 
                               F.col("M_Score").cast("string")
                           ))

# COMMAND ----------

# Create segments based on RFM scores
rfm_segments_df = rfm_df.withColumn(
    "RFM_Segment",
    F.when((F.col("R_Score") >= 4) & (F.col("F_Score") >= 4) & (F.col("M_Score") >= 4), "Champions")
    .when((F.col("R_Score") >= 3) & (F.col("F_Score") >= 3) & (F.col("M_Score") >= 3), "Loyal")
    .when((F.col("R_Score") >= 4) & (F.col("F_Score") >= 3) & (F.col("M_Score") < 3), "Potential Loyalists")
    .when((F.col("R_Score") >= 3) & (F.col("F_Score") < 3) & (F.col("M_Score") >= 3), "Big Spenders")
    .when((F.col("R_Score") < 2) & (F.col("F_Score") >= 4) & (F.col("M_Score") >= 4), "At Risk")
    .when((F.col("R_Score") < 2) & (F.col("F_Score") >= 3) & (F.col("M_Score") < 3), "Need Attention")
    .when((F.col("R_Score") < 2) & (F.col("F_Score") < 3) & (F.col("M_Score") < 3), "About to Leave")
    .when((F.col("R_Score") >= 4) & (F.col("F_Score") < 2) & (F.col("M_Score") < 2), "New")
    .when((F.col("R_Score") < 3) & (F.col("F_Score") < 3) & (F.col("M_Score") >= 3), "High Value Prospects")
    .otherwise("Others")
)
rfm_segments_df.cache()

# COMMAND ----------

# Display RFM segment distribution
print("RFM Segment Distribution:")
display(rfm_segments_df.groupBy("RFM_Segment").count().orderBy(F.desc("count")))

# COMMAND ----------

display(rfm_segments_df.limit(10))

# COMMAND ----------

gold_customers_df = gold_customers_df.join(rfm_segments_df.select("CustomerID", "RFM_Segment", "Recency" ), on="CustomerID", how="left")
gold_customers_df.cache()
gold_customers_count = gold_customers_df.count()

# COMMAND ----------

display(gold_customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer
# MAGIC Save the enriched Customer Data to the Gold Layer.

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {gold_customers_count} customer records to Gold Layer: {gold_customers_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_customers_path, recurse=True)

# Apply Delta optimizations
gold_customers_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_customers_path)

print(f"Successfully wrote Customer Data to Gold Layer: {gold_customers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC Read back from Gold Layer to Verify the Data was Written Correctly.

# COMMAND ----------

# Read the Gold Data
tmp_gold_customers_df = spark.read.format(file_format).load(gold_customers_path)

# Create a new DataFrame from the RDD with the schema to ensure nullable properties are respected
gold_customers_verify_df = spark.createDataFrame(tmp_gold_customers_df.rdd, gold_customers_schema)
gold_customers_verify_df.cache()

# Compare record counts
gold_customers_verify_count = gold_customers_verify_df.count()
    
print(f"Gold customers record count: {gold_customers_verify_count}")
print(f"Records match: {gold_customers_count == gold_customers_verify_count}")

# COMMAND ----------

display(gold_customers_verify_df.limit(10))

# COMMAND ----------

# Display final schema
gold_customers_verify_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Customer Age vs. Spending Patterns

# COMMAND ----------

gold_customers_verify_df.createOrReplaceTempView("customer_segments")

# COMMAND ----------

# Analyze spending by age group
age_spending_analysis = gold_customers_verify_df.groupBy("AgeGroup") \
    .agg(
        F.count("CustomerID").alias("CustomerCount"),
        F.round(F.sum("TotalSpending"), 2).alias("TotalRevenue"),
        F.round(F.avg("TotalSpending"), 2).alias("AvgRevenuePerCustomer"),
        F.round(F.avg("AverageSpending"), 2).alias("AvgOrderValue")
    ) \
    .orderBy("AgeGroup")

display(age_spending_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segment Analysis by Age Group

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Segment analysis by age group
# MAGIC SELECT 
# MAGIC     AgeGroup,
# MAGIC     RFM_Segment,
# MAGIC     COUNT(*) AS CustomerCount,
# MAGIC     ROUND(AVG(TotalSpending), 2) AS AvgSpending,
# MAGIC     ROUND(AVG(NoOfInvoices), 2) AS AvgPurchases
# MAGIC FROM 
# MAGIC     customer_segments
# MAGIC GROUP BY 
# MAGIC     AgeGroup, RFM_Segment
# MAGIC ORDER BY 
# MAGIC     AgeGroup, AvgSpending DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segment analysis by country

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Segment analysis by country
# MAGIC SELECT 
# MAGIC     Country,
# MAGIC     RFM_Segment,
# MAGIC     COUNT(*) AS CustomerCount,
# MAGIC     ROUND(SUM(TotalSpending), 2) AS TotalRevenue,
# MAGIC     ROUND(AVG(TotalSpending), 2) AS AvgSpending
# MAGIC FROM 
# MAGIC     customer_segments
# MAGIC GROUP BY 
# MAGIC     Country, RFM_Segment
# MAGIC ORDER BY 
# MAGIC     Country, TotalRevenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze metrics by segment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze metrics by segment
# MAGIC CREATE OR REPLACE TEMP VIEW segment_metrics AS
# MAGIC SELECT 
# MAGIC     RFM_Segment,
# MAGIC     COUNT(*) AS CustomerCount,
# MAGIC     ROUND(SUM(TotalSpending), 2) AS TotalRevenue,
# MAGIC     ROUND(100 * SUM(TotalSpending) / (SELECT SUM(TotalSpending) FROM customer_segments), 2) AS RevenuePercentage,
# MAGIC     ROUND(AVG(Recency), 0) AS AvgDaysSinceLastPurchase,
# MAGIC     ROUND(AVG(NoOfInvoices), 1) AS AvgPurchaseCount,
# MAGIC     ROUND(AVG(AverageSpending), 2) AS AvgOrderValue,
# MAGIC     ROUND(100 * SUM(CASE WHEN IsParent THEN 1 ELSE 0 END) / COUNT(*), 1) AS ParentPercentage
# MAGIC FROM 
# MAGIC     customer_segments
# MAGIC GROUP BY 
# MAGIC     RFM_Segment
# MAGIC ORDER BY 
# MAGIC     TotalRevenue DESC;

# COMMAND ----------

# Save the result in a DataFrame
segment_metrics_df = spark.sql("SELECT * FROM segment_metrics")
segment_metrics_df.cache()
segment_metrics_count = segment_metrics_df.count()
display(segment_metrics_df)

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {segment_metrics_count} records to Gold Layer: {gold_segment_metrics_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_segment_metrics_path, recurse=True)

# Apply Delta optimizations
segment_metrics_df.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_segment_metrics_path)

print(f"Successfully wrote records to Gold Layer: {gold_segment_metrics_path}")
