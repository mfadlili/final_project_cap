from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta, timezone

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

yesterday = str(datetime.now(timezone.utc) - timedelta(days=1)).split()[0]

path = 'hdfs://localhost:9000/user/fadlil/staging/'+yesterday+".parquet"
df = spark.read.parquet(path)

# Create Dimension Table
spark.sql("""
CREATE TABLE IF NOT EXISTS dwh.dim_broker(
sk_broker INTEGER,
broker STRING
)
""")

# Change list_date column format into date format
df = df.withColumn("list_date", regexp_replace(col("list_date"), "T", " "))
df = df.withColumn("list_date", regexp_replace(col("list_date"), "Z", ""))
df = df.withColumn("date", split(df["list_date"], " ")[0])
df = df.withColumn("timestamp", split(df["list_date"], " ")[1])
# df = df.withColumn("list_date", date_format("list_date", "dd/MM/yyyy HH:mm:ss"))
#df.show(5)

#Dimension Broker Transformation
windowSpec = Window.orderBy("broker")
df_dim_broker = spark.table("dwh.dim_broker")
new_data_broker = df.selectExpr('broker').distinct().dropna()
filtered_new_data_broker = new_data_broker.exceptAll(df_dim_broker.select(['broker']))
filtered_new_data_broker = filtered_new_data_broker.withColumn("sk_broker", row_number().over(windowSpec)+df_dim_broker.count())
# combined_df_dim_broker = df_dim_broker.union(filtered_new_data_broker.select(['sk_broker','broker']))
# append_df_dim_broker = combined_df_dim_broker.exceptAll(df_dim_broker)

filtered_new_data_broker.select(['sk_broker','broker']).orderBy('sk_broker').createOrReplaceTempView('filtered_new_dim_broker')
spark.sql("INSERT INTO dwh.dim_broker SELECT * FROM filtered_new_dim_broker")

# data = [("John", 25), ("Jane", 30), ("Mike", 35)]
# df_data = spark.createDataFrame(data, ["name", "age"])
# df_data.createOrReplaceTempView('coba_data')
# spark.sql("INSERT INTO dwh.coba SELECT * FROM coba_data")

# filtered_new_data_broker.show(5)
# combined_df_dim_broker.show(5)
# append_df_dim_broker.orderBy('sk_broker').show(5)