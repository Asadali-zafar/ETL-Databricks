# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalakegen212/raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####spark reader API. checkout the documenttion to checkout the parameters

# COMMAND ----------

circuits_dataframe = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/datalakegen212/raw/circuits.csv")

# COMMAND ----------

display(circuits_dataframe)

# COMMAND ----------

circuits_dataframe.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now we have to apply dataframe Api of spark to transform the data
# MAGIC #### 1) apply proper schema : two methods for that; 1) let spark infer the schema (datatypes) in read statement
# MAGIC #### it will slow the read action in very large data.
# MAGIC #### 2) use pyspark modules to structure the datatypes of each fields.

# COMMAND ----------



# COMMAND ----------

circuits_dataframe.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("location", StringType(), True),
                                       StructField("country", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng", DoubleType(), True),
                                       StructField("alt", DoubleType(), True),
                                       StructField("url", StringType(), True),
                                       ])

# COMMAND ----------

circuits_dataframe = spark.read.option("header", True).schema(circuits_schema).csv("/mnt/datalakegen212/raw/circuits.csv")

# COMMAND ----------

circuits_dataframe.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Selecting specific columns of interest we can also drop by using dataframe drop api

# COMMAND ----------

circuits_dataframe_selected = circuits_dataframe.select("circuitId", "circuitRef", "name", "location", "country", "lat","lng", "alt", "url")

# COMMAND ----------

# MAGIC %md
# MAGIC we can also use fileds by df.columname without string, or df["column name"] just like pandas and also by using col funcion . but these end three have advantage over the first one, which is if we want to apply the functions on the column name then we can do that by the end three methods.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_dataframe_selected = circuits_dataframe.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"),col("lng"), col("alt"))

# COMMAND ----------

display(circuits_dataframe_selected)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns

# COMMAND ----------

df_selected_renamed = circuits_dataframe_selected.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("race_country", "country") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude")

# COMMAND ----------

display(df_selected_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding audit column with injestion time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC "production" is not a column object it is a literal object. so this lit function will make the literal object into column object.

# COMMAND ----------

final_df = df_selected_renamed.withColumn("injestion_date", current_timestamp())\
.withColumn("environment",lit("Production"))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### write data to datalake in parquet format

# COMMAND ----------

final_df.write.parquet("/mnt/datalakegen212/processed")

# COMMAND ----------

# MAGIC %md
# MAGIC we can see that only one part because we have one node. if multiple nodes than there will be many parts of the files. not that the file is in the datalake we can simply use spark read Api to read the data from data lake.

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalakegen212/processed"))

# COMMAND ----------

df_parquet = spark.read.parquet("/mnt/datalakegen212/processed")

# COMMAND ----------

display(df_parquet)

# COMMAND ----------


