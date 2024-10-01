# Databricks notebook source
# MAGIC %md
# MAGIC #The below code will read the CSV file and create a dataframe with predefined schema

# COMMAND ----------


file_location = "/FileStore/tables/DataSamplewithpipe-3.csv"
file_type = "csv"


first_row_is_header = "false"
delimiter = "|"
data_schema = "stationId string,issueTime timestamp,forecastValidFrom timestamp,forecastValidTo timestamp,windDirection integer,windSpeed double,cloudCoverage string,type string"


df = spark.read.format(file_type) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .schema(data_schema) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #The below code will apply the transformation and create another dataframe
# MAGIC * from_json will parse the cloudCoverage column based on json_schema
# MAGIC * explode will transform each element into multiple rows
# MAGIC * explode and sequence are used to create the range of timestamp records from forecastValidFrom to forecastValidTo with every hour interval and flattened the data into multiple rows
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,from_json,explode,sequence,to_timestamp,expr,regexp_replace

json_schema = "array<struct<cover:integer,baseHeight:integer,type:string>>"

df1 = df \
.withColumn("cloud_coverage_new",from_json(col("cloudCoverage"), json_schema)) \
.withColumn("cloud_coverage_exploded",explode(col("cloud_coverage_new"))) \
.withColumn("cloud_cover",col("cloud_coverage_exploded.cover")) \
.withColumn("cloud_baseHeight",col("cloud_coverage_exploded.baseHeight")) \
.withColumn("each_hour",expr("explode(sequence(forecastValidFrom,forecastValidTo,interval 1 hour))")) \
.select("each_hour","windDirection","windSpeed","cloud_cover","cloud_baseHeight")

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #The below code will apply aggregation on the dataframe df1
# MAGIC * mode function will return the most frequent value from windDirection Column

# COMMAND ----------

from pyspark.sql.functions import mode,avg,max,round
##.select("each_hour","windDirection","windSpeed","cloud_cover","cloud_baseHeight")
df2 = df1 \
.groupBy("each_hour") \
.agg(mode("windDirection").alias("common_wind_dir"),round(avg("windSpeed"),2).alias("avg_wind_speed"),max("cloud_cover").alias("max_cloud_cover"),round(avg("cloud_baseHeight"),2).alias("avg_cloud_baseHeight"))

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #The data will be inserted into a Delta table

# COMMAND ----------

permanent_table_name = "weather_pattern"

df2.write.format("delta").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_pattern
