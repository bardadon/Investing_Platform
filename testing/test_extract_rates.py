import pytest
from dags.python.extract_rates import extract_rates, extract_rates_dictionary, create_dataframe, export_to_csv
import configparser
import pandas as pd
import json 


# Configs
config = configparser.ConfigParser()
config.read('/projects/stock_analysis_platform/dags/python/pipeline.conf')
api_key = config.get('fixer_io_api_key', 'api_key')


#def test_extract_rates_canCall():

 #  rates = extract_rates(api_key = api_key, start_date='2022-01-01', end_date='2022-02-01')
  # with open('/projects/stock_analysis_platform/rates.txt', 'w') as write_file:
   #     write_file.write(rates)


def test_extractRates_generate_dates():
    start_date='2022-01-01'
    end_date='2022-02-01'
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    assert dates[0].date().isoformat() == start_date 
    assert dates[-1].date().isoformat() == end_date 

def test_extractRates_CreateRatesDataFrame():

    with open('/projects/stock_analysis_platform/rates.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()
        
    rates = extract_rates_dictionary(results=results)
    rates_df = create_dataframe(rates, start_date='2022-01-01', end_date='2022-02-01')
    
    assert rates_df.iloc[0,0] == 4.176782

def test_extractRates_ExportToCSV():

    with open('/projects/stock_analysis_platform/rates.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()
        
    rates = extract_rates_dictionary(results=results)
    rates_df = create_dataframe(rates, start_date='2022-01-01', end_date='2022-02-01')
    
    export_to_csv(rates_df)
    
    pd.read_csv('/projects/stock_analysis_platform/rates.csv')
    