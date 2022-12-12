import requests
import configparser
import pandas as pd
import json 


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

def create_dataframe(rates:dict, start_date:str, end_date:str) -> pd.DataFrame:

    '''
    Create a dataframe for the first day. 
    Iterate over the dates from the start_date to the end_date, create temp df for each date and append to original df.

    Args:
        rates:dict
        start_date:str
        end_date:str
    Returns:
        rates_df(pd.DataFrame)
    '''

    first_day_df = pd.DataFrame(data = rates.get(str(start_date)), index = [0])
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    for date in dates.date:

        temp_df = pd.DataFrame(data = rates.get(str(date)), index = [0])
        first_day_df = pd.concat([first_day_df,temp_df], axis=0)

    return first_day_df

def export_to_csv(rates_df:pd.DataFrame) -> None:

    '''
    Export DataFrame to CSV
    
    Args:
        - rates_df(pd.DataFrame)
    Returns:
        - None
    '''

    rates_df.to_csv('/projects/stock_analysis_platform/rates.csv')



if __name__ == '__main__':
    
    # Extract rates from Fixer.io API
    #results = extract_rates(api_key = api_key, start_date='2022-01-01', end_date='2022-02-01')    
    
    # Grab rates from text file
    with open('/projects/stock_analysis_platform/dags/python/rates.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results=results)
    rates_df = create_dataframe(rates, start_date='2022-01-01', end_date='2022-02-01')
    export_to_csv(rates_df)