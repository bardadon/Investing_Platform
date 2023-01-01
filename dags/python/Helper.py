import configparser
import datetime
import requests
import json
import pandas as pd
import numpy as np
import os

# Configs
config = configparser.ConfigParser()
config.read('dags/python/pipeline.conf')
api_key = config.get('fixer_io_api_key', 'api_key')

# Set google configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/ServiceKey_GoogleCloud.json'


##                      ##
## Populating Platform  ##
##                      ##

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

# Dag #4 - Load Raw data to Cloud Storage
def load_to_google_storage():

    from google.cloud import storage

    # Create a storage client
    storage_client = storage.Client()

    try:
        # Create a Cloud Storage Bucket
        storage_client.create_bucket(bucket_or_name='stock_analysis_platform')
    except Exception:
        # Grab the bucket
        bucket = storage_client.get_bucket(bucket_or_name='stock_analysis_platform')

    # Create blob
    blob = bucket.blob('rates.csv')

    # Upload to bucket
    blob.upload_from_filename('dags/rates.csv')

# Dag #5 - Process Raw Data
def process_rates(rates_location = 'dags/rates.csv') -> pd.DataFrame:

    '''
    Process the rates DataFrame. 
    Convert format from this:
        
        - date|AED|AFN|ALL|AMD|ANG ... etc

    To this:

        - date|symbol|rate

    Args:
        - rates_DataFrame_location(str) - Location of the rates CSV file.
    Returns:
        - new_df(pd.DataFrame) - The new DataFrame.
    '''
    
    df = pd.read_csv(rates_location, index_col='Unnamed: 0')

    # Create a new DataFrame
    new_df = pd.DataFrame(columns=['date', 'symbol', 'rate'])

    # Set values
    symbols = df.T.index
    start_date = np.min(df.index)
    end_date = np.max(df.index)
    dates = pd.date_range(start_date, end_date, freq='D')
    arr = np.array([])

    # Create a series of dates.
    # Each date is repeated as many times as there are symbols
    repeated_dates = np.tile(dates, len(symbols))
    dates_series = pd.Series(repeated_dates).sort_values()

    # Append date and symbol data to new DataFrame
    new_df['date'] = dates_series
    new_df['symbol'] =  np.tile(symbols, len(dates))

    # Create a numpy array of all the rates from the original DataFrame
    for i in range(len(dates)):
        arr = np.append(arr = arr, values=df.iloc[i, :])
    
    # Append the rates to the rate column in the new DataFrame
    new_df['rate'] = arr

    # Sort DataFrame by date and reset index
    new_df.sort_values(by='date', ascending=False, inplace=True)
    new_df = new_df.reset_index(drop=['index'])

    # Export DataFrame to CSV
    new_df.to_csv('dags/processed_rates.csv')
    
    # Return the new DataFrame
    return new_df

# Dag #6 - Load process data to BigQuery
def load_to_bigquery() -> None:

    '''
    Read the processed rates into a Pandas DataFrame.
    Export the DataFrame to Google BigQuery

    Pre-requisites(Go to Google Console):
        - Create a Dataset at BigQuery called: "Forex_Platform"
        - Create a table called: "rates"
    '''

    import pandas as pd
    from google.cloud import bigquery

    # Fetch the processed rates DataFrame
    rates = pd.read_csv('dags/processed_rates.csv', index_col='Unnamed: 0')

    # Create a BigQuery client
    bigquery_client = bigquery.Client()

    # Get or Create a data set 
    try:
        # Create the dataset
        dataset = bigquery_client.create_dataset('Forex_Platform')
    except Exception:
        # Grab the data set "Forex_Platform"
        dataset = bigquery_client.get_dataset('Forex_Platform')

    # Set table details
    table = dataset.table('rates')
    table.schema = [
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('symbol', 'TEXT', mode='REQUIRED'),
        bigquery.SchemaField('rate', 'DECIMAL', mode='REQUIRED')
    ]

    # Get or Create a table
    try:
        # Create the table
        table = bigquery_client.create_table(table)
    except Exception:
        # Grab the table 
        table = bigquery_client.get_table(table)

    # Load the DataFrame "rates" to the table "rates"
    bigquery_client.load_table_from_dataframe(dataframe=rates, destination=table)

##                      ##
## Insert new rates     ##
##                      ##

def grab_next_date():

    # Grab latest date from the file name of the lastest CSV file
    os.chdir(path='dags/data')
    latest_csv_file = os.listdir()[0]
    latest_day = latest_csv_file[len('processed_rates_'):-4]

    # Calculate next day
    start_date_datetime = datetime.datetime.fromisoformat(latest_day)
    td = datetime.timedelta(days=1)
    next_date = datetime.datetime.strftime((start_date_datetime + td), format = '%Y-%m-%d')

    return next_date

# Dag #3 - Create a DataFrame
def create_EOD_dataframe(rates: dict, start_date: str) -> pd.DataFrame:

    eod_data = rates.get(str(start_date))
    rates_df = pd.DataFrame(eod_data, index = [0])
    rates_df.index = [start_date]
    return rates_df

# Dag #4 - Process Raw Data
def process_EOD_rates(rates_df) -> pd.DataFrame:

    '''
    Process the rates DataFrame. 
    Convert format from this:
        
        - date|AED|AFN|ALL|AMD|ANG ... etc

    To this:

        - date|symbol|rate

    Args:
        - rates_df(pd.DataFrame) - The rates DataFrame
    Returns:
        - new_df(pd.DataFrame) - The processed DataFrame.
    '''
    # Create a new DataFrame
    new_df = pd.DataFrame(columns=['date', 'symbol', 'rate'])

    # Grab the symbols and rates from old DataFrame
    symbols = rates_df.T.index
    start_date = np.min(rates_df.index)
    arr = rates_df.values[0]
    
    # Set some values
    repeated_dates = np.tile(start_date, len(symbols))
    dates_series = pd.Series(repeated_dates).sort_values()

    # Append the date, symbols and rates to new DataFrame
    new_df['date'] = dates_series
    new_df['symbol'] =  symbols
    new_df['rate'] = arr

    # Reset index
    new_df = new_df.reset_index(drop=['index'])
    # Export as CSV. Add current day to the name
    new_df.to_csv(f'processed_rates_{start_date}.csv')

    # Return the new DataFrame
    return new_df


# Dag #6 - Load process data to BigQuery
def load_to_bigquery_EOD(new_df) -> None:

    '''
    Read the processed rates into a Pandas DataFrame.
    Export the DataFrame to Google BigQuery

    Pre-requisites(Go to Google Console):
        - Create a Dataset at BigQuery called: "Forex_Platform"
        - Create a table called: "rates"
    '''

    from google.cloud import bigquery

    # Create a BigQuery client
    bigquery_client = bigquery.Client()

    # Grab the data set "Forex_Platform"
    dataset = bigquery_client.get_dataset('Forex_Platform')

    # Fetch details about the table "rates"
    project = dataset.project
    dataset = dataset.dataset_id
    table_name = 'rates'
    table_id = project + '.' + dataset + '.' + table_name

    # Grab the table 
    table = bigquery_client.get_table(table_id)

    # Load the DataFrame "rates" to the table "rates"
    bigquery_client.load_table_from_dataframe(dataframe=new_df, project=project, destination=table)