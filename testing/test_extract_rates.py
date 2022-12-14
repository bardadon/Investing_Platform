import pytest
from dags.python.Helper import extract_rates, extract_rates_dictionary, create_dataframe, load_to_google_storage
import configparser
import pandas as pd
import json


# Configs
config = configparser.ConfigParser()
config.read('/projects/stock_analysis_platform/dags/python/pipeline.conf')
api_key = config.get('fixer_io_api_key', 'api_key')



#def test_extract_rates_canCall():

 #   rates = extract_rates(api_key = api_key, start_date='2022-01-01', end_date='2022-01-02')
  #  with open('/projects/stock_analysis_platform/rates_test.txt', 'w') as write_file:
   #     write_file.write(rates)

def test_extractRates_generate_dates():
    start_date='2022-01-01'
    end_date='2022-01-02'
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    assert dates[0].date().isoformat() == start_date 
    assert dates[-1].date().isoformat() == end_date 

def test_extractRates_extract_rates_dictionary():

    with open('/projects/stock_analysis_platform/rates_test.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results)

    assert '2022-01-01' in rates.keys()



def test_extractRates_create_dataframe():

    with open('/projects/stock_analysis_platform/rates_test.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results)

    start_date = "2022-01-01"
    end_date = "2022-01-02"

    # Call the function
    df = create_dataframe(rates, start_date, end_date)


    assert df.iloc[0:1, 0].values == 4.176782 

