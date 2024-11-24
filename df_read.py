# Databricks notebook source
print(";el")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("create a dataframe").gerOrCreate()

data = [(1, "Suresh"), (2, "ravi")]
columns = ["id", "Name"]

df = spark.createDataFrame(data, columns)

df.show()