import pyspark
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql.session import SparkSession


spark = SparkSession.builder.appName('test').getOrCreate()

data = [[2021, "test", "Albany", "M", 42]]
columns = ["Year", "First_Name", "County", "Sex", "Count"]

df1 = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
display(df1) # The display() method is specific to Databricks notebooks and provides a richer visualization.
# df1.show() The show() method is a part of the Apache Spark DataFrame API and provides basic visualization.
df_csv = spark.read.csv(f"work/Baby_Names__Beginning_2007.csv",
  header=True,
  inferSchema=True,
  sep=",")
display(df_csv)
df_csv.show(5)
