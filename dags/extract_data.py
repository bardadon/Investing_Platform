from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from python.Helper import *


import configparser
from datetime import datetime
import pandas as pd



# Configs
config = configparser.ConfigParser()
config.read('dags/python/pipeline.conf')
api_key = config.get('fixer_io_api_key', 'api_key')


default_args = {
    'start_date':datetime(2022,1,1),
    'schedule_interval':'0 0 * * *'
}


@dag(
    dag_id='load_rates', 
    default_args= default_args, 
    catchup=False,
    tags=['test']
)

def extract_data():

    # Dag #1 - extract rates dictionary
    @task()
    def dag1_extract_rates(api_key, start_date, end_date):
        results = extract_rates(api_key, start_date, end_date)
        return results


    # Dag #2 - extract the rates dictionary
    @task()
    def dag2_extract_rates_dictionary(results:str) -> dict:
        rates = extract_rates_dictionary(results) 
        return rates


    # Dag #3 - Create a dataframe 
    @task()
    def dag3_create_dataframe(rates: dict, start_date: str, end_date: str) -> pd.DataFrame:
        create_dataframe(rates, start_date, end_date)

    # Dag #4 - Process DataFrame and create a new one
    @task()
    def dag4_process_rates():
        process_rates()

    # Dag #5 - Create Google cloud storage bucket
    @task()
    def dag5_load_to_google_storage():
        load_to_google_storage()


    # Dependencies
    results = dag1_extract_rates(api_key = api_key, start_date='2022-01-01', end_date='2022-01-02')
    rates = dag2_extract_rates_dictionary(results=results)
    dag3_create_dataframe(rates, start_date='2022-01-01', end_date='2022-01-02') >> dag4_process_rates() >> dag5_load_to_google_storage()
    

# Instantiating the DAG
extract_data = extract_data()