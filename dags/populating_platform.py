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

# Choose the dates to populate
start_date = '2022-01-01'
end_date = '2022-03-01'

default_args = {
    'start_date':datetime.fromisoformat(start_date)
}


@dag(
    dag_id='populating_platform', 
    default_args= default_args, 
    catchup=False,
    tags=['Used to populate the platform with an initial batch of data']
)

def populating_platform():

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

    # Dag #4 - Create Google cloud storage bucket
    @task()
    def dag4_load_to_google_storage():
        load_to_google_storage()

    
    # Dag #5 - Process DataFrame and create a new one
    @task()
    def dag5_process_rates():
        process_rates()

    # Dag #6 - Load process data to BigQuery
    @task()
    def dag6_load_to_bigquery():
        load_to_bigquery()


    # Dependencies
    results = dag1_extract_rates(api_key = api_key, start_date=start_date, end_date=end_date)
    rates = dag2_extract_rates_dictionary(results=results)
    dag3_create_dataframe(rates, start_date=start_date, end_date=end_date) \
    >> [dag4_load_to_google_storage(), dag5_process_rates()] >> dag6_load_to_bigquery()

# Instantiating the DAG
populating_platform = populating_platform()