# Databricks notebook source
from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType 


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Read in Dataset

# COMMAND ----------

df_laptimes = spark.read.csv("s3://columbia-gr5069-main/raw/lap_times.csv",header = True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_drivers = spark.read.csv("s3://columbia-gr5069-main/raw/drivers.csv", header=True)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform

# COMMAND ----------

df_drivers = df_drivers.withColumn("age",datediff(current_date(),df_drivers.dob)/365)

# COMMAND ----------

df_drivers = df_drivers.withColumn("age",datediff(current_date(),df_drivers.dob)/365)
df_drivers = df_drivers.withColumn("age",df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

df_drivers = df_drivers.withColumn("age",datediff(current_date(),df_drivers.dob)/365)

# COMMAND ----------

display(df_drivers)


# COMMAND ----------

df_lap_drivers = df_drivers.select('driverId','driverRef','code','forename','surname','nationality','age').join(df_laptimes,on=['driverId'])


# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_races = spark.read.csv("s3://columbia-gr5069-main/raw/races.csv",header = True)

# COMMAND ----------

display(df_races)

# COMMAND ----------

df_lap_drivers =df_lap_drivers.join(df_races.select('raceId','year','name'),on = ['raceId']).drop('raceId','driverId')

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregation by Age

# COMMAND ----------


df_agg_age = df_lap_drivers.groupby('age').agg(avg('milliseconds').alias('avg_lap_time'))

# COMMAND ----------

display(df_agg_age)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store it as S3

# COMMAND ----------


