from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

import requests
from datetime import datetime, timedelta, timezone
import csv
from final_project_query import create_db_staging, create_table_property, insert_table_property

yesterday = str(datetime.now(timezone.utc) - timedelta(days=1)).split()[0]
linux_path = '/home/fadlil/property/'

default_args = {'owner':'fadlil', 
                'retries':1, 
                'retry_delay':timedelta(minutes=25)}

with DAG(dag_id = 'dag_final_project',
        default_args=default_args,
        schedule = '@daily',
        start_date = datetime(2023,5,30)
         ) as dag:
    
    get_data_task = BashOperator(task_id='get_data',
                                bash_command = "python3 /mnt/c/Users/user/Documents/airflow/dags/final_project_function.py")
    
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

get_data_task >> create_database_staging_task >> create_table_property_task >> insert_table_property_task