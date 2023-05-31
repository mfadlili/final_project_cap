from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

import requests
from datetime import date, datetime, timedelta
import pandas as pd
from final_project_query import create_db_staging, create_table_property, insert_table_property

yesterday = date.today() - timedelta(days = 1)
linux_path = '/home/fadlil/property/'

default_args = {'owner':'fadlil', 
                'retries':1, 
                'retry_delay':timedelta(minutes=25)}

url = ["https://us-real-estate.p.rapidapi.com/v2/for-sale", "https://us-real-estate.p.rapidapi.com/v2/for-rent"]

state_city = {"state": ["NY", "CA", "TX", "FL", "PA", "CA"], "city": ["New York", "Los Angeles", "Houston", "Miami", "Philadelphia", "San Diego"]}
headers = {
	"X-RapidAPI-Key": "87a28bece1mshcdfa17d9b4a65ffp154842jsnd998fd341d5c",
	"X-RapidAPI-Host": "us-real-estate.p.rapidapi.com"
}

def get_data(url, headers, state_city):
    result = []
    for link in url:
        for i in range(len(state_city["state"])):
            params = {"offset":"0","limit":"200","state_code":state_city["state"][i], "city":state_city["city"][i],"sort":"newest"}
            response = requests.get(link, headers=headers, params=params)
        if response.status_code == 200:
            result.append(response.json())
    
    tampung = []
    for city_data in result:
        for data in city_data['data']['home_search']['results']:
            dict_tampung = {}

            dict_tampung['property_id'] = data['property_id']

            try:
                dict_tampung['list_date'] = data['list_date']
            except:
                dict_tampung['list_date'] = None

            try:
                dict_tampung['list_price'] = data['list_price']
            except:
                dict_tampung['list_price'] = None

            try:
                dict_tampung['feature'] = data['tags']
            except:
                dict_tampung['feature'] = None

            try:
                dict_tampung['tax_id'] = data['tax_record']['public_record_id']
            except:
                dict_tampung['tax_id'] = None

            try:
                dict_tampung['photo'] = data['primary_photo']['href']
            except:
                dict_tampung['photo'] = None

            dict_tampung['line'] = data['location']['address']['line']
            dict_tampung['status'] = data['status']

            try:
                dict_tampung['broker'] = data['products']['brand_name']
            except:
                dict_tampung['broker'] = None
            
            try:
                dict_tampung['year_built'] = data['description']['year_built']
            except:
                dict_tampung['year_built'] = None

            try:
                dict_tampung['total_sqft'] = data['description']['lot_sqft']
            except:
                dict_tampung['total_sqft'] = None

            try:
                dict_tampung['building_sqft'] = data['description']['sqft']
            except:
                dict_tampung['building_sqft'] = None

            try:
                dict_tampung['baths'] = data['description']['baths']
            except:
                dict_tampung['baths'] = None
            
            try:
                dict_tampung['state'] = data['location']['address']['state']
            except:
                dict_tampung['state'] = None
            
            try:
                dict_tampung['county'] = data['location']['county']['name']
            except:
                dict_tampung['county'] = None
            
            try:
                dict_tampung['city'] = data['location']['address']['city']
            except:
                dict_tampung['city'] = None
            
            try:
                dict_tampung['postal_code'] = data['location']['address']['postal_code']
            except:
                dict_tampung['postal_code'] = None

            try:
                dict_tampung['stories'] = data['description']['stories']
            except:
                dict_tampung['stories'] = None
            
            try:
                dict_tampung['beds'] = data['description']['beds']
            except:
                dict_tampung['beds'] = None
            
            try:
                dict_tampung['garage'] = data['description']['garage']
            except:
                dict_tampung['garage'] = None
            
            try:
                dict_tampung['type'] = data['description']['type']
            except:
                dict_tampung['type'] = None

            try:
                dict_tampung['name'] = data['description']['name']
            except:
                dict_tampung['name'] = None

            tampung.append(dict_tampung)

    df = pd.DataFrame(tampung)

    df.to_parquet(linux_path+str(yesterday)+".parquet")

with DAG(dag_id = 'dag_final_project',
        default_args=default_args,
        schedule = '@daily',
        start_date = datetime(2023,5,30)
         ) as dag:
    
    get_data_task = PythonOperator(task_id='get_data',
                           python_callable=get_data,
                           op_kwargs={"url": url, "headers": headers, "state_city": state_city})
    
    create_database_staging_task = HiveOperator(
        task_id='create_database_staging',
        hql = create_db_staging,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table_property_task = HiveOperator(
        task_id='create_table_property',
        hql = create_table_property,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_table_property_task = HiveOperator(
        task_id='insert_table_property',
        hql=insert_table_property,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )