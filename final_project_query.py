from datetime import timedelta, date

date_yesterday = date.today() - timedelta(days = 1)

create_db_staging = """
CREATE DATABASE IF NOT EXISTS staging;
"""

create_table_property = """
CREATE EXTERNAL TABLE IF NOT EXISTS staging.property (
property_id STRING,
list_date STRING,
list_price DOUBLE,
feature ARRAY<STRING>,
tax_id STRING,
photo STRING,
line STRING,
status STRING,
broker STRING,
year_built DOUBLE,
total_sqft DOUBLE,
building_sqft DOUBLE,
num_baths DOUBLE,
state STRING,
county STRING,
city STRING,
postal_code STRING
)
STORED AS PARQUET LOCATION '/user/fadlil/staging' 
tblproperties ("skip.header.line.count"="1");
"""

insert_table_property = f"""
LOAD DATA LOCAL INPATH '/home/fadlil/property/{str(date_yesterday)}.parquet'
INTO TABLE staging.property;
"""