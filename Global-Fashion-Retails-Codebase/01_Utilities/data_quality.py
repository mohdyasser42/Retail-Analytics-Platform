# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework
# MAGIC
# MAGIC This Module Provides a Comprehensive Set of Functions for Validating Data Quality Thresholds and Validation Rules Across The Retail Analytics Platform.

# COMMAND ----------

# Importing Necessary Libraries
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, TimestampType, DoubleType, LongType, BooleanType

# COMMAND ----------

# Custom Schemas for each Dataset for Loading Data from Bronze Layer

# Store Schema 
stores_schema = StructType([
    StructField("StoreID", IntegerType(), False),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("StoreName", StringType(), True),
    StructField("NumberOfEmployees", IntegerType(), True),
    StructField("ZIPCode", StringType(), True),
    StructField("Latitude", DecimalType(10, 6), True),
    StructField("Longitude", DecimalType(10, 6), True)
])

# Employees Schema
employees_schema = StructType([
    StructField("EmployeeID", IntegerType(), False),
    StructField("StoreID", IntegerType(), True),
    StructField("Name", StringType(), False),
    StructField("Position", StringType(), True)
])

# Products Schema
products_schema = StructType([
    StructField("ProductID", IntegerType(), False),
    StructField("Category", StringType(), True),
    StructField("SubCategory", StringType(), True),
    StructField("DescriptionPT", StringType(), True),
    StructField("DescriptionDE", StringType(), True),
    StructField("DescriptionFR", StringType(), True),
    StructField("DescriptionES", StringType(), True),
    StructField("DescriptionEN", StringType(), True),
    StructField("DescriptionZH", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("Sizes", StringType(), True),
    StructField("ProductionCost", DecimalType(10, 4), True)  # Using DecimalType for SMALLMONEY
])

# Discounts Schema
discounts_schema = StructType([
    StructField("StartDate", DateType(), True),
    StructField("EndDate", DateType(), True),
    StructField("Discount", DecimalType(10, 2), True),
    StructField("Description", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("SubCategory", StringType(), True)
])

# Customers Schema
customers_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Email", StringType(), True),
    StructField("Telephone", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("DateOfBirth", DateType(), True),
    StructField("JobTitle", StringType(), True)
])

# Transactions Schema
transactions_schema = StructType([
    StructField("InvoiceID", StringType(), False),
    StructField("Line", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("Size", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("UnitPrice", DecimalType(10, 4), True),  # Using DecimalType for SMALLMONEY
    StructField("Quantity", IntegerType(), True),
    StructField("Timestamp", TimestampType(), True),  # Using TimestampType for SMALLDATETIME
    StructField("Discount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(10, 4), True),  # Using DecimalType for SMALLMONEY
    StructField("StoreID", IntegerType(), True),
    StructField("EmployeeID", IntegerType(), True),
    StructField("Currency", StringType(), True),
    StructField("CurrencySymbol", StringType(), True),
    StructField("SKU", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("InvoiceTotal", DecimalType(10, 4), True)  # Using DecimalType for SMALLMONEY
])

# COMMAND ----------

# Custom Schemas for each Dataset for Loading Data from Silver Layer

# Discounts Schema
discounts_schema2 = StructType([
    StructField("StartDate", DateType(), True),
    StructField("EndDate", DateType(), True),
    StructField("DiscountID", IntegerType(), False),
    StructField("Discount", DecimalType(10, 2), True),
    StructField("Description", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("SubCategory", StringType(), True)
])

# COMMAND ----------

# Custom Schemas for each Dataset for Loading Data from Gold Layer

# schema for the exchange rates
exchange_rates_schema = StructType([
    StructField("RateDate", DateType(), False),
    StructField("BaseCurrency", StringType(), False),
    StructField("TargetCurrency", StringType(), False),
    StructField("ExchangeRate", DoubleType(), False)    
])

# Invoice_fact Schema
invoice_fact_schema = StructType([
    StructField("InvoiceID", StringType(), False),
    StructField("CustomerID", IntegerType(), True),
    StructField("Date", DateType(), True),
    StructField("Time", StringType(), True), # Use StringType for time representation
    StructField("StoreID", IntegerType(), True),
    StructField("StoreCountry", StringType(), True),
    StructField("StoreCity", StringType(), True),
    StructField("EmployeeID", IntegerType(), True),
    StructField("TotalQuantity", IntegerType(), True),
    StructField("Currency", StringType(), True),
    StructField("CurrencySymbol", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("InvoiceTotal", DecimalType(10, 4), True),
    StructField("InvoiceTotalUSD", DecimalType(10, 4), True)
])
    
# Invoice_line_items Schema
invoice_line_items_schema = StructType([
    StructField("LineItemID", IntegerType(), False),
    StructField("InvoiceID", StringType(), False),
    StructField("Line", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("Category", StringType(), True),
    StructField("SubCategory", StringType(), True),
    StructField("Size", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("UnitPrice", DecimalType(10, 4), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DecimalType(10, 2), True),
    StructField("DiscountID", IntegerType(), True),
    StructField("SKU", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("LineTotal", DecimalType(10, 4), True)
])

# Customers Schema
gold_customers_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Email", StringType(), True),
    StructField("Telephone", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("DateOfBirth", DateType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("AgeGroup", StringType(), True),
    StructField("IsParent", BooleanType(), True),
    StructField("TotalSpending", DecimalType(20, 4), True),
    StructField("NoOfInvoices", IntegerType(), True),
    StructField("AverageSpending", DecimalType(14, 4), True)
])

gold_stores_schema = StructType([
    # Original fields from silver
    StructField("StoreID", IntegerType(), False),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("StoreName", StringType(), True),
    StructField("NumberOfEmployees", IntegerType(), True),
    StructField("ZIPCode", StringType(), True),
    StructField("Latitude", DecimalType(10,6), True),
    StructField("Longitude", DecimalType(10,6), True),
    # New analytical fields
    StructField("TotalSales", DecimalType(10,2), True),
    StructField("TotalTransactions", IntegerType(), True),
    StructField("TotalReturns", IntegerType(), True),
    StructField("AverageMonthlyUSD", DecimalType(10,2), True),
    StructField("ReturnRate", DecimalType(5,2), True),
    StructField("MonthsOfOperation", DecimalType(5,1), True),
    StructField("SalesPerEmployee", DecimalType(10,2), True)
])
