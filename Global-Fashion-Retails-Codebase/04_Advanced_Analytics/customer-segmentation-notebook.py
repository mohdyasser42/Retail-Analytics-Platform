# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Segmentation: Gold Data to Analytics Models
# MAGIC
# MAGIC This notebook implements comprehensive customer segmentation using:
# MAGIC 1. RFM Analysis (Recency, Frequency, Monetary)
# MAGIC 2. K-means Clustering
# MAGIC 3. Combined Segmentation Model
# MAGIC
# MAGIC The results are saved to the Gold layer for use in dashboards and analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import All Utilities and Libraries

# COMMAND ----------

# MAGIC %run ../01_Utilities/config

# COMMAND ----------

# MAGIC %run ../01_Utilities/common_functions

# COMMAND ----------

# MAGIC %run ../01_Utilities/data_quality

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
import pandas as pd
import numpy as np
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Layer Data

# COMMAND ----------

# Load needed Gold layer datasets
gold_customers_path = get_gold_path("customers")
gold_invoice_fact_path = get_gold_path("invoice_fact")
gold_customer_segments_path = get_gold_path("customer_segments")

# COMMAND ----------

# Processing parameters
write_mode = WRITE_MODE
file_format = FILE_FORMATS["gold"]

print(f"Processing Customer Segmentation:")
print(f"- Source: {gold_customers_path}")
print(f"- Destination: {gold_customer_segments_path}")
print(f"Write mode: {write_mode}, File format: {file_format}")

# COMMAND ----------

# Load Gold customers data with analytical attributes
print("Loading Gold Customers Data...")
tmp_customers_df = spark.read.format(file_format).load(gold_customers_path)
gold_customers_df = spark.createDataFrame(tmp_customers_df.rdd, gold_customers_schema)
print(f"Loaded {gold_customers_df.count()} customer records") 

# Load Gold invoice data for recency calculation
print("\nLoading Gold Invoice Fact Data...")
tmp_invoice_df = spark.read.format(file_format).load(gold_invoice_fact_path)
gold_invoice_df = spark.createDataFrame(tmp_invoice_df.rdd, invoice_fact_schema)
print(f"Loaded {gold_invoice_df.count()} invoice records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: RFM Analysis (Recency, Frequency, Monetary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Recency (days since last purchase)

# COMMAND ----------

# Register tables as temporary views for SQL operations
gold_customers_df.createOrReplaceTempView("gold_customers")
gold_invoice_df.createOrReplaceTempView("gold_invoice_fact")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view with the last purchase date for each customer
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_last_purchase AS
# MAGIC SELECT 
# MAGIC     CustomerID,
# MAGIC     MAX(Date) AS LastPurchaseDate
# MAGIC FROM 
# MAGIC     gold_invoice_fact
# MAGIC WHERE 
# MAGIC     TransactionType = 'Sale'
# MAGIC GROUP BY 
# MAGIC     CustomerID

# COMMAND ----------

# Use a Particular Date for Recency Calculation
current_date = "2025-05-14"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate RFM metrics
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_rfm AS
# MAGIC SELECT 
# MAGIC     c.CustomerID,
# MAGIC     DATEDIFF('2025-05-14', COALESCE(lp.LastPurchaseDate, '2025-05-14')) AS Recency,
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
    .when((F.col("R_Score") >= 3) & (F.col("F_Score") >= 3) & (F.col("M_Score") >= 3), "Loyal Customers")
    .when((F.col("R_Score") >= 4) & (F.col("F_Score") >= 3) & (F.col("M_Score") < 3), "Potential Loyalists")
    .when((F.col("R_Score") >= 3) & (F.col("F_Score") < 3) & (F.col("M_Score") >= 3), "Big Spenders")
    .when((F.col("R_Score") < 2) & (F.col("F_Score") >= 4) & (F.col("M_Score") >= 4), "At Risk")
    .when((F.col("R_Score") < 2) & (F.col("F_Score") >= 3) & (F.col("M_Score") < 3), "Need Attention")
    .when((F.col("R_Score") < 2) & (F.col("F_Score") < 3) & (F.col("M_Score") < 3), "About to Leave")
    .when((F.col("R_Score") >= 4) & (F.col("F_Score") < 2) & (F.col("M_Score") < 2), "New Customers")
    .when((F.col("R_Score") < 3) & (F.col("F_Score") < 3) & (F.col("M_Score") >= 3), "High Value Prospects")
    .otherwise("Others")
)

# COMMAND ----------

# Display RFM segment distribution
print("RFM Segment Distribution:")
display(rfm_segments_df.groupBy("RFM_Segment").count().orderBy(F.desc("count")))

# COMMAND ----------

display(rfm_segments_df)

# COMMAND ----------

rfm_segments_df.cache()

# COMMAND ----------

final_customers_df = gold_customers_df.join(rfm_segments_df.select("CustomerID", "RFM_Segment"), on="CustomerID", how="left")
final_customers_df.cache()
final_customers_df.count()

# COMMAND ----------

display(final_customers_df)

# COMMAND ----------

# Check distinct values of RFM_Segment column
distinct_rfm_segments = final_customers_df.select("RFM_Segment").distinct()
display(distinct_rfm_segments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: K-means Clustering for Behavioral Segmentation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Data for K-means Clustering

# COMMAND ----------

# Select and prepare features for clustering
features_df = gold_customers_df.select(
    "CustomerID",
    "TotalSpending",
    "NoOfInvoices",
    "AverageSpending",
    "Age"
).na.fill({
    "TotalSpending": 0,
    "NoOfInvoices": 0,
    "AverageSpending": 0,
    "Age": gold_customers_df.select(F.avg("Age")).first()[0]  # Fill with average age
})

# COMMAND ----------

# Create feature vector
assembler = VectorAssembler(
    inputCols=["TotalSpending", "NoOfInvoices", "AverageSpending", "Age"],
    outputCol="features"
)

# Create scaler for standardizing features
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures",
    withStd=True,
    withMean=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find Optimal Number of Clusters

# COMMAND ----------

# Function to evaluate KMeans
def evaluate_kmeans(df, features_col, k_values):
    evaluator = ClusteringEvaluator(featuresCol=features_col)
    results = []
    
    for k in k_values:
        kmeans = KMeans(k=k, featuresCol=features_col)
        model = kmeans.fit(df)
        predictions = model.transform(df)
        
        # Calculate Silhouette Score
        silhouette = evaluator.evaluate(predictions)
        
        # Calculate WSSSE (Within-Set Sum of Squared Errors)
        wssse = model.summary.trainingCost
        
        results.append((k, silhouette, wssse))
        print(f"K={k}: Silhouette Score={silhouette}, WSSSE={wssse}")
    
    return results

# COMMAND ----------

# Prepare data pipeline
pipeline = Pipeline(stages=[assembler, scaler])
prepared_data = pipeline.fit(features_df).transform(features_df)



# Try different numbers of clusters
k_values = [2, 3, 4, 5, 6, 7, 8]
cluster_evaluation = evaluate_kmeans(prepared_data, "scaledFeatures", k_values)

# COMMAND ----------

# Choose optimal K (let's say k=5 based on the evaluation results)
optimal_k = 5  # This would normally be selected based on evaluation results

# COMMAND ----------

print(cluster_evaluation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final K-means Model and Predictions

# COMMAND ----------

# Create final K-means model with optimal k
kmeans = KMeans().setK(optimal_k).setFeaturesCol("scaledFeatures").setPredictionCol("cluster")
model = kmeans.fit(prepared_data)

# Make predictions
cluster_predictions = model.transform(prepared_data)

# COMMAND ----------

# Analyze the clusters
cluster_summary = cluster_predictions.groupBy("cluster") \
    .agg(
        F.count("CustomerID").alias("CustomerCount"),
        F.round(F.avg("TotalSpending"), 2).alias("AvgSpending"),
        F.round(F.avg("NoOfInvoices"), 2).alias("AvgFrequency"),
        F.round(F.avg("AverageSpending"), 2).alias("AvgOrderValue"),
        F.round(F.avg("Age"), 2).alias("AvgAge")
    ) \
    .orderBy("cluster")

display(cluster_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign Cluster Names Based on Characteristics

# COMMAND ----------

# Define a function to assign descriptive names to clusters
def assign_cluster_names(cluster_id, summary_data):
    # Extract cluster statistics from summary_data
    # This is a simplified example - in practice, you'd have a more sophisticated mapping
    cluster_stats = summary_data.filter(F.col("cluster") == cluster_id).collect()[0]
    
    # Determine characteristics based on statistics
    if cluster_stats["AvgSpending"] > 1000 and cluster_stats["AvgFrequency"] > 5:
        return "High-Value Frequent Shoppers"
    elif cluster_stats["AvgSpending"] > 1000:
        return "Big Spenders"
    elif cluster_stats["AvgFrequency"] > 5:
        return "Frequent Shoppers"
    elif cluster_stats["AvgAge"] < 30:
        return "Young Occasional Shoppers"
    elif cluster_stats["AvgOrderValue"] > 200:
        return "Premium Shoppers"
    else:
        return "Standard Customers"

# COMMAND ----------

# Create a mapping from cluster ID to cluster name
cluster_names_mapping = {}
for i in range(optimal_k):
    cluster_names_mapping[i] = assign_cluster_names(i, cluster_summary)

# Convert mapping to DataFrame for joining
cluster_names_df = spark.createDataFrame(
    [(k, v) for k, v in cluster_names_mapping.items()],
    ["cluster", "ClusterName"]
)

# Add descriptive names to the clusters
named_clusters = cluster_predictions.join(cluster_names_df, on="cluster", how="left")

# COMMAND ----------

# Display clusters with their names
display(named_clusters.groupBy("ClusterName")
        .count()
        .orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Combined Segmentation Model

# COMMAND ----------

# Join RFM segments with K-means clusters
combined_segments = rfm_segments_df.select(
    "CustomerID", 
    "RFM_Score", 
    "RFM_Segment", 
    "Recency", 
    "Frequency", 
    "Monetary"
).join(
    named_clusters.select(
        "CustomerID", 
        "cluster", 
        "ClusterName"
    ), 
    on="CustomerID", 
    how="inner"
)

# COMMAND ----------

# Join with original customer data to create final customer segments
customer_segments = combined_segments.join(
    gold_customers_df,
    on="CustomerID",
    how="inner"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final Customer Segment

# COMMAND ----------

# Create a single integrated segment that combines RFM and cluster insights
customer_segments = customer_segments.withColumn(
    "CustomerSegment",
    F.when(F.col("RFM_Segment") == "Champions", "VIP")
    .when(F.col("RFM_Segment") == "Loyal Customers", "Loyal")
    .when(F.col("RFM_Segment") == "At Risk" , "At Risk")
    .when(F.col("ClusterName").like("%Frequent%"), "Regular")
    .when(F.col("ClusterName").like("%Young%"), "Young")
    .when(F.col("ClusterName").like("%Premium%"), "Premium")
    .when(F.col("RFM_Segment") == "New Customers", "New")
    .when(F.col("Recency") > 180, "Inactive")
    .otherwise("Standard")
)

# COMMAND ----------

# Display distribution of final customer segments
print("Final Customer Segment Distribution:")
display(customer_segments.groupBy("CustomerSegment").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Customer Segments to Gold Layer

# COMMAND ----------

# Select final columns for the customer segments table
final_customer_segments = customer_segments.select(
    "CustomerID", 
    "Name", 
    "Email",
    "Gender",
    "Age",
    "AgeGroup",
    "City",
    "Country",
    "JobTitle",
    "IsParent",
    "TotalSpending",
    "NoOfInvoices",
    "AverageSpending",
    "Recency",
    "Frequency",
    "Monetary",
    "RFM_Segment",
    "ClusterName",
    "CustomerSegment"
)

# COMMAND ----------

# Count of records for verification
segment_count = final_customer_segments.count()

# COMMAND ----------

# Save as Delta format in the Gold Layer
print(f"Writing {segment_count} customer segment records to Gold Layer: {gold_customer_segments_path}")

# Delete existing Delta files
dbutils.fs.rm(gold_customer_segments_path, recurse=True)

# Apply Delta optimizations
final_customer_segments.coalesce(1) \
    .write \
    .format(file_format) \
    .mode(write_mode) \
    .options(**DELTA_OPTIONS) \
    .save(gold_customer_segments_path)

print(f"Successfully wrote Customer Segments to Gold Layer: {gold_customer_segments_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification and Final Analysis

# COMMAND ----------

# Read back the customer segments
final_segments_df = spark.read.format(file_format).load(gold_customer_segments_path)
print(f"Loaded {final_segments_df.count()} customer segment records")

# COMMAND ----------

# Register for SQL queries
final_segments_df.createOrReplaceTempView("customer_segments")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Segment analysis by age group
# MAGIC SELECT 
# MAGIC     AgeGroup,
# MAGIC     CustomerSegment,
# MAGIC     COUNT(*) AS CustomerCount,
# MAGIC     ROUND(AVG(TotalSpending), 2) AS AvgSpending,
# MAGIC     ROUND(AVG(NoOfInvoices), 2) AS AvgPurchases
# MAGIC FROM 
# MAGIC     customer_segments
# MAGIC GROUP BY 
# MAGIC     AgeGroup, CustomerSegment
# MAGIC ORDER BY 
# MAGIC     AgeGroup, AvgSpending DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Segment analysis by country
# MAGIC SELECT 
# MAGIC     Country,
# MAGIC     CustomerSegment,
# MAGIC     COUNT(*) AS CustomerCount,
# MAGIC     ROUND(SUM(TotalSpending), 2) AS TotalRevenue,
# MAGIC     ROUND(AVG(TotalSpending), 2) AS AvgSpending
# MAGIC FROM 
# MAGIC     customer_segments
# MAGIC GROUP BY 
# MAGIC     Country, CustomerSegment
# MAGIC ORDER BY 
# MAGIC     Country, TotalRevenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze metrics by segment
# MAGIC SELECT 
# MAGIC     CustomerSegment,
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
# MAGIC     CustomerSegment
# MAGIC ORDER BY 
# MAGIC     TotalRevenue DESC
