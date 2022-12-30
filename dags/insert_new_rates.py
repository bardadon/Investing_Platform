from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from python.Helper import *


import configparser
from datetime import datetime
import datetime
import pandas as pd
import json
import os

# Configs
config = configparser.ConfigParser()
config.read('dags/python/pipeline.conf')
api_key = config.get('fixer_io_api_key', 'api_key')

# Set default settings
default_args = {
    'start_date':datetime.datetime(2022,1,1),
    'scheduled_interval':'0 8 * * *' # Run every day at 08:00 AM
}

@dag(
    dag_id='insert_new_rates', 
    default_args= default_args, 
    catchup=False,
    tags=['Insert daily data']
)

def insert_new_dates():

    # Dag #1 - extract rates dictionary
    @task()
    def dag1_extract_rates(api_key, start_date, end_date):
        results = extract_rates(api_key, start_date=next_date, end_date=next_date)
        return results


    # Dag #2 - extract the rates dictionary
    @task()
    def dag2_extract_rates_dictionary(results:str) -> dict:
        rates = extract_rates_dictionary(results) 
        return rates

    # Dag #3 - Create a dataframe 
    @task()
    def dag3_create_EOD_dataframe(rates: dict, start_date: str) -> pd.DataFrame:
        rates_df = create_EOD_dataframe(rates, start_date=next_date)

        # Return the DataFrame as JSON
        return rates_df.to_json()

    # Dag #4 - Process DataFrame and create a new one
    @task()
    def dag4_process_EOD_rates(rates_df):

        # Convert JSON back to pd.DataFrame
        df = json.loads(rates_df)
        rates_df = pd.DataFrame(df)

        new_df = process_EOD_rates(rates_df)
        return new_df.to_json()

    # Dag #5 - Load process data to BigQuery
    @task()
    def dag5_load_to_bigquery_EOD(new_df):

        # Convert JSON back to pd.DataFrame
        df = json.loads(new_df)
        new_df = pd.DataFrame(df)

        load_to_bigquery_EOD(new_df = new_df)


    # Dependencies
    next_date = grab_next_date()
    results = dag1_extract_rates(api_key = api_key, start_date=next_date, end_date=next_date)
    rates = dag2_extract_rates_dictionary(results=results)
    rates_df = dag3_create_EOD_dataframe(rates, start_date=next_date)
    new_df = dag4_process_EOD_rates(rates_df)
    dag5_load_to_bigquery_EOD(new_df=new_df)
    
    

# Instantiating the DAG
insert_new_dates = insert_new_dates()
