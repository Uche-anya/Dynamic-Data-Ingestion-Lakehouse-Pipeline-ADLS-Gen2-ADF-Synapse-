# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Silver Layer transformation 

# COMMAND ----------S

# MAGIC %md
# MAGIC ### 1.1 Access data using Azure application

# COMMAND ----------

# Set Spark configurations to authenticate to ADLS Gen2 using OAuth and Service Principal
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", oauth2_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Load data from all sources
                                      
# COMMAND ----------

df_calender = spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_calender.show()

# COMMAND ----------


df_customer = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Customers")

df_customer.show()


# COMMAND ----------

df_product_category= spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Product_Categories")

df_product_category.show()

# COMMAND ----------

df_products = spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Products")

df_products.show()

# COMMAND ----------

df_returns = spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Returns")

df_returns.show()

# COMMAND ----------

df_sales_years = spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Sales*")

df_sales_years.show() 

# COMMAND ----------

df_territories = spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/AdventureWorks_Territories")

df_territories.show()

# COMMAND ----------

df_product_subcategories = spark.read.format("csv")\
    .option("header", "true")\
     .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworksazure.dfs.core.windows.net/Product_Subcategories")

df_product_subcategories.show()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.1 Calender Transformation

# COMMAND ----------

df_calender = df_calender.withColumn("Month", month(col("Date")))\
                   .withColumn("Year", year(col("Date")))
df_calender.display()

# COMMAND ----------

df_calender.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Calender").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.2 Customers transformation

# COMMAND ----------

df_customer = df_customer.withColumn(
    "FullName", 
    concat(
        col("Prefix"), 
        lit(' '), 
        col("FirstName"), 
        lit(' '), 
        col("LastName")
    ))
df_customer.display()

# COMMAND ----------

df_customer = df_customer.withColumn(
    "FullName", 
    concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName"))
)

df_customer.display()


# COMMAND ----------

df_customer.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Customer").save()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.3 Sub-Categories transformation

# COMMAND ----------

df_product_subcategories.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Product_Subcategories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.4 Products transformation

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])
                
df_products.display()

# COMMAND ----------

df_products.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Products").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.5 Return transformation

# COMMAND ----------

df_returns.display()


# COMMAND ----------

df_returns.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Returns").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.6 Territory transformation

# COMMAND ----------

df_territories.show()

# COMMAND ----------

df_territories.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.7 Sales Transformation

# COMMAND ----------

df_sales_years.show()

# COMMAND ----------

df_sales_years = df_sales_years.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales_years.show()

# COMMAND ----------

df_sales_years = df_sales_years.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales_years.show()

# COMMAND ----------

df_sales_years.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@adventureworksazure.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

df_sales_years.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()


# COMMAND ----------

df_territories.display()

# COMMAND ----------

df_product_category.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC