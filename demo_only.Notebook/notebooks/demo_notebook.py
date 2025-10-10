# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Notebook - Fabric & Local Compatible
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC - Spark session setup (auto in Fabric, local via builder)
# MAGIC - Reading CSV input
# MAGIC - Writing Spark output
# MAGIC - Optional DB read (SQLite local, SQL Fabric in UAT)
# MAGIC 
# MAGIC Works both in:
# MAGIC - 🧱 Local Python environment
# MAGIC - ⚡ Microsoft Fabric via Git integration

# COMMAND ----------

import os
import sys

from src.db_utils import load_env, get_local_dataframe, get_fabric_dataframe
from src.spark_utils import create_spark_session, read_csv, write_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Environment and Initialize Spark

# COMMAND ----------

env = load_env()
spark = create_spark_session(app_name="FabricLocalDemo")

print(f"✅ Environment: {env}")
print(f"✅ Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Input CSV (from `/data/input`) and Transform

# COMMAND ----------

csv_input = "./data/input/users.csv"
csv_output = "./data/output/users_out"

df_csv = read_csv(spark, csv_input)
print("📥 CSV contents:")
df_csv.show()

# Simple transformation
df_filtered = df_csv.filter(df_csv.department == "IT")

# COMMAND ----------

# write_csv(df_filtered, csv_output)
print(f"💾 Filtered output written to: {csv_output}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Data from Database (Optional Section)

# COMMAND ----------

if env == "dev":
    df_db = get_local_dataframe()
    print("🧱 Loaded data from local SQLite database:")
else:
    df_db = get_fabric_dataframe()
    print("⚡ Loaded data from Fabric SQL or Lakehouse:")

print(df_db.head())

# COMMAND ----------

# MAGIC %
