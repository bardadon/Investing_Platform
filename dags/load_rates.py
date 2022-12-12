from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

import configparser
from datetime import datetime
import requests
import json
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

    # Add this dag at the end of the project.
    # Dag #1 - extract rates dictionary
 #   @task()
  #  def extract_rates(api_key:str, start_date:str, end_date:str) -> str:

   #     '''
    #    Extract Forex rates from Fixer.io. 
        
     #   Args:
      #      - api_key:str, start_date:str, end_date:str
       # Returns
        #    - result:dict
        #'''

  #      url = f"https://api.apilayer.com/fixer/timeseries?start_date={start_date}&end_date={end_date}"

   #     payload = {}
    #    headers= {
     #   "apikey": api_key
      #  }

       # response = requests.request("GET", url, headers=headers, data = payload)
        #results = response.text

        #return results

    # Dag #2 - extract the rates dictionary
    @task()
    def extract_rates_dictionary(results:str) -> dict:

        '''
        Extract the rates dictionary from Fixer.io response.

        Args:
            - result = Fixer.io reponse
        Returns
            - rates = rates dictionary
        '''

        data = json.loads(results)
        rates = data['rates']
        return rates

    # Dag #3 - Create a dataframe 
    @task()
    def create_dataframe(rates: dict, start_date: str, end_date: str) -> pd.DataFrame:

        # Create a dataframe with the columns and indices from the first day's data
        first_day_data = rates.get(str(start_date))
        if first_day_data is None:
            # Return an empty dataframe if there is no data for the start date
            return pd.DataFrame()
        first_day_df = pd.DataFrame(data=first_day_data, index=[0])

        # Iterate over the dates from the start_date to the end_date
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        for date in dates.date:
            data = rates.get(str(date))
            if data is None:
                # Skip the date if there is no data for it
                continue
            # Append the data for the date to the dataframe
            first_day_df = first_day_df.append(data, ignore_index=True)
        
        first_day_df.to_csv('dags/rates.csv')





    # Dependencies
    #results = extract_rates(api_key = api_key, start_date='2022-01-01', end_date='2022-01-05')
    with open('dags/python/rates.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results=results)
    create_dataframe(rates, start_date='2022-01-01', end_date='2022-01-05')

# Instantiating the DAG
extract_data = extract_data()