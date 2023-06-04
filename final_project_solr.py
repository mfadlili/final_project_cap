from pyspark.sql import SparkSession
import pysolr
import subprocess
import requests
import json
from bs4 import BeautifulSoup as bs
from datetime import timedelta, datetime, timezone

yesterday = str(datetime.now(timezone.utc) - timedelta(days=1)).split()[0]

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

tables = ['dim_date', 'dim_broker', 'dim_type', 'dim_year_built', 'dim_location', 'fact_property_listing']

solr_url = 'http://localhost:8983/solr'

for table in tables:
    url = 'http://localhost:8983/solr/admin/cores?action=STATUS&core={}'
    response = requests.get(url.format(table))
    soup = bs(response.content, 'lxml')

    if not soup.findAll('str', {'name':'name'}):
        print(f"Create core {table}.")
        solr_command = '/home/fadlil/hadoop/Solr/bin/solr create_core -c {}'
        command = solr_command.format(table)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print(f"Create core {table} success")
    else:
        print(f"Core: {table} already exists.")
    
    if table == 'dim_date' :
        df = spark.sql(f"select * from dwh.{table} where date='{yesterday}'")
    else :
        df = spark.sql(f"select * from dwh.{table} where insert_date='{yesterday}'")
    
    dict_data = [json.loads(row) for row in df.toJSON().collect()]
    solr = pysolr.Solr(solr_url + '/' + table)
    solr.add(dict_data)

    solr.commit()

spark.stop()
    