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

# Dag #1 - extract rates dictionary
def extract_rates(api_key:str, start_date:str, end_date:str) -> str:

    '''
    Extract Forex rates from Fixer.io. 

    Args:
    - api_key:str, start_date:str, end_date:str
    Returns
    - result:dict
    '''

    url = f"https://api.apilayer.com/fixer/timeseries?start_date={start_date}&end_date={end_date}"

    payload = {}
    headers= {
    "apikey": api_key
    }

    response = requests.request("GET", url, headers=headers, data = payload)
    results = response.text

    return results

# Dag #2 - extract the rates dictionary
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
def create_dataframe(rates: dict, start_date: str, end_date: str, export_to_csv=True) -> pd.DataFrame:

    # Create a dataframe with the columns and indices from the first day's data
    first_day_data = rates.get(str(start_date))
    if first_day_data is None:
        # Return an empty dataframe if there is no data for the start date
        return pd.DataFrame()
    first_day_df = pd.DataFrame()

    # Iterate over the dates from the start_date to the end_date
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    for date in dates.date:
        data = rates.get(str(date))
        if data is None:
            # Skip the date if there is no data for it
            continue
            # Append the data for the date to the dataframe
        first_day_df = first_day_df.append(data, ignore_index=True)

    # Add dates to the dataframe
    first_day_df.index = dates.date

    if export_to_csv == True:
        first_day_df.to_csv('dags/rates.csv')
        
    return first_day_df

# Dag #4 - Create Google cloud storage bucket
def load_to_google_storage():

    from google.cloud import storage
    import os

    # Set google configuration
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dags/ServiceKey_GoogleCloud.json'

    # Create a storage client
    storage_client = storage.Client()

    # Grab the bucket
    bucket = storage_client.get_bucket(bucket_or_name='stock_analysis_platform')

    # Create blob
    blob = bucket.blob('rates.csv')

    # Upload to bucket
    blob.upload_from_filename('dags/rates.csv')


def process_rates():
    
    df = pd.read_csv('dags/rates.csv', index_col='Unnamed: 0')
    
    
    return df


if __name__ == '__main__':
    #results = extract_rates(api_key = api_key, start_date='2022-01-01', end_date='2022-01-02')
    #rates = extract_rates_dictionary(results=results)
    #create_dataframe(rates, start_date='2022-01-01', end_date='2022-01-02')
    
    df = process_rates()
    new_df = pd.DataFrame(columns=['Date', 'Symbol', 'Rate'])
    
    print(df)
    print(new_df.iloc[0:1, 0])

    
    currency_values = df.iloc[:, 0].values
    currency_name = df.columns[0]
    currency_name = df.columns[0]
    print(currency_values, currency_name)
    new_df[currency_name] = currency_values

    print(new_df)


    #load_to_google_storage()