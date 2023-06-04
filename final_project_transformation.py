from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta, timezone
from pyspark.sql.types import LongType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

yesterday = str(datetime.now(timezone.utc) - timedelta(days=1)).split()[0]

path = 'hdfs://localhost:9000/user/fadlil/staging/'+yesterday+".parquet"
#path = 'hdfs://localhost:9000/user/fadlil/staging/2023-06-01.parquet'
df = spark.read.parquet(path)

# Change property_id data type
df = df.withColumn("property_id", col("property_id").cast(LongType()))

# Change year_built data type
df = df.withColumn("year_built", col("year_built").cast("integer"))

# Change baths data type
df = df.withColumn("baths", col("baths").cast("integer"))

# Change stories data type
df = df.withColumn("stories", col("stories").cast("integer"))

# Change beds data type
df = df.withColumn("beds", col("beds").cast("integer"))

# Change garage data type
df = df.withColumn("garage", col("garage").cast("integer"))

# Change list_date column format into date format
df = df.withColumn("list_date", regexp_replace(col("list_date"), "T", " "))
df = df.withColumn("list_date", regexp_replace(col("list_date"), "Z", ""))
df = df.withColumn("date", split(df["list_date"], " ")[0]) 
df = df.withColumn("list_time", date_format(df["list_date"], "HH:mm:ss"))

# Create Dimension Date Table
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.dim_date(
sk_date LONG,
date DATE,
month DATE,
year INTEGER
)
""")

# Dimension Date Transformation
df_date = df.select(['date']).distinct()
df_date = df_date.withColumn("month", date_trunc("month", df_date["date"]))
df_date = df_date.withColumn("month", df_date["month"].cast("date"))
df_date = df_date.withColumn("year", year(df_date["month"]))

windowSpec = Window.orderBy("date")
df_dim_date = spark.table("dwh.dim_date")
filtered_date = df_date.exceptAll(df_dim_date.select(['date', 'month', 'year']))
filtered_date = filtered_date.withColumn("sk_date", row_number().over(windowSpec) + df_dim_date.count())

filtered_date.select(['sk_date', 'date', 'month', 'year']).createOrReplaceTempView('filtered_date')
spark.sql("INSERT INTO dwh.dim_date SELECT * FROM filtered_date")

# Create Dimension Broker Table
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.dim_broker(
sk_broker LONG,
broker STRING,
insert_date DATE
)
""")
          
#Dimension Broker Transformation
windowSpec = Window.orderBy("broker")
df_dim_broker = spark.table("dwh.dim_broker")
new_data_broker = df.selectExpr('broker').distinct().dropna()
filtered_new_data_broker = new_data_broker.exceptAll(df_dim_broker.select(['broker']))
if filtered_new_data_broker.count() > 0:
    filtered_new_data_broker = filtered_new_data_broker.withColumn("sk_broker", row_number().over(windowSpec) + df_dim_broker.count())
    filtered_new_data_broker = filtered_new_data_broker.withColumn("insert_date", date_sub(current_date(), 1))

    filtered_new_data_broker.select(['sk_broker','broker', 'insert_date']).orderBy('sk_broker').createOrReplaceTempView('filtered_new_dim_broker')
    spark.sql("INSERT INTO dwh.dim_broker SELECT * FROM filtered_new_dim_broker")

# Create Dimension Type Table
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.dim_type(
sk_type INTEGER,
type STRING,
insert_date DATE
)
""")

#Dimension Type Transformation
windowSpec = Window.orderBy("type")
df_dim_type = spark.table("dwh.dim_type")
new_data_type = df.selectExpr('type').distinct().dropna()
filtered_new_data_type = new_data_type.exceptAll(df_dim_type.select(['type']))
if filtered_new_data_type.count() > 0:
    filtered_new_data_type = filtered_new_data_type.withColumn("sk_type", row_number().over(windowSpec) + df_dim_type.count())
    filtered_new_data_type = filtered_new_data_type.withColumn("insert_date", date_sub(current_date(), 1))

    filtered_new_data_type.select(['sk_type','type', 'insert_date']).orderBy('sk_type').createOrReplaceTempView('filtered_new_dim_type')
    spark.sql("INSERT INTO dwh.dim_type SELECT * FROM filtered_new_dim_type")

# Create Dimension Year Built Table
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.dim_year_built(
sk_year_built INTEGER,
year_built INTEGER,
insert_date DATE
)
""")

#Dimension Year Built Transformation
windowSpec = Window.orderBy("year_built")
df_dim_year_built = spark.table("dwh.dim_year_built")
new_data_year_built = df.selectExpr('year_built').distinct().dropna()
filtered_new_data_year_built = new_data_year_built.exceptAll(df_dim_year_built.select(['year_built']))
if filtered_new_data_year_built.count() > 0:
    filtered_new_data_year_built = filtered_new_data_year_built.withColumn("sk_year_built", row_number().over(windowSpec) + df_dim_year_built.count())
    filtered_new_data_year_built = filtered_new_data_year_built.withColumn("insert_date", date_sub(current_date(), 1))

    filtered_new_data_year_built.select(['sk_year_built','year_built', 'insert_date']).orderBy('sk_year_built').createOrReplaceTempView('filtered_new_dim_year_built')
    spark.sql("INSERT INTO dwh.dim_year_built SELECT * FROM filtered_new_dim_year_built")

# Create Dimension Location Table
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.dim_location(
sk_location LONG,
state STRING,
county STRING,
city STRING,
postal_code STRING,
insert_date DATE
)
""")

#Dimension Location Transformation
windowSpec = Window.orderBy("city")
df_dim_location = spark.table("dwh.dim_location")
new_data_location = df.selectExpr('state','county','city', 'postal_code').distinct().dropna()
filtered_new_data_location = new_data_location.exceptAll(df_dim_location.select(['state','county','city', 'postal_code']))
if filtered_new_data_location.count() > 0:
    filtered_new_data_location = filtered_new_data_location.withColumn("sk_location", row_number().over(windowSpec) + df_dim_location.count())
    filtered_new_data_location = filtered_new_data_location.withColumn("insert_date", date_sub(current_date(), 1))

    filtered_new_data_location.select(['sk_location','state','county','city', 'postal_code', 'insert_date']).orderBy('sk_location').createOrReplaceTempView('filtered_new_dim_location')
    spark.sql("INSERT INTO dwh.dim_location SELECT * FROM filtered_new_dim_location")

# Create Fact Table Property Listing
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.fact_property_listing(
insert_date DATE,
list_time STRING,
property_id LONG,
name STRING,
list_price DOUBLE,
feature ARRAY<STRING>,
photo STRING,
line STRING,
status STRING,
total_sqft DOUBLE,
building_sqft DOUBLE, 
baths INTEGER,
stories INTEGER, 
beds INTEGER, 
garage INTEGER,
tax_id STRING, 
sk_date INTEGER, 
sk_location INTEGER, 
sk_type INTEGER, 
sk_year_built INTEGER, 
sk_broker INTEGER
)
""")

# Fact Table Transformation
df_dim_date = spark.table("dwh.dim_date")
df_dim_broker = spark.table("dwh.dim_broker")
df_dim_type = spark.table("dwh.dim_type")
df_dim_year_built = spark.table("dwh.dim_year_built")
df_dim_location = spark.table("dwh.dim_location")
df_fact_table = spark.table("dwh.fact_property_listing")

joined_df = df.join(df_dim_broker, on=["broker"], how="left")
joined_df = joined_df.join(df_dim_date, on=["date"], how="left")
joined_df = joined_df.join(df_dim_type, on=["type"], how="left")
joined_df = joined_df.join(df_dim_location, on=['state','county','city', 'postal_code'], how="left")
joined_df = joined_df.join(df_dim_year_built, on=["year_built"], how="left")
joined_df.select(['date', 'list_time' , 'property_id', 'name', 'list_price', 'feature', 'photo', 'line', 'status', 'total_sqft', 'building_sqft', 'baths', 'stories', 'beds', 'garage', 'tax_id', 'sk_date', 'sk_location', 'sk_type', 'sk_year_built', 'sk_broker']).createOrReplaceTempView('new_fact_data')
spark.sql("INSERT INTO dwh.fact_property_listing SELECT * FROM new_fact_data")
