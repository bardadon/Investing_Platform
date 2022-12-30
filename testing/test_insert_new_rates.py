import pytest
from dags.python.Helper import *
import configparser
import pandas as pd
import json


# Configs
config = configparser.ConfigParser()
config.read('/projects/stock_analysis_platform/dags/python/pipeline.conf')
api_key = config.get('fixer_io_api_key', 'api_key')

#def test_insertRates_canExtractOneDay():
 #   rates = extract_rates(api_key = api_key, start_date='2022-03-02', end_date='2022-03-02')
    
  #  with open('/projects/stock_analysis_platform/testing/rates_EOD_test.txt', 'w') as write_file:
   #     write_file.write(rates)
    #    write_file.close

    #assert '2022-03-02' in rates

    

def test_insertRates_extract_rates_dictionary():

    with open('/projects/stock_analysis_platform/testing/rates_EOD_test.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results)

    assert '2022-03-04' in rates.keys()

def test_insertRates_create_EOD_DataFrame():

    with open('/projects/stock_analysis_platform/testing/rates_EOD_test.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results)

    rates_df = create_EOD_dataframe(rates, start_date='2022-03-02')
    
    assert rates_df.index == '2022-03-02' 
    assert rates_df.AED[0] == 4.082038

def test_extractRates_canReadDataframeAndCreateNew():

    # Initialize values
    with open('/projects/stock_analysis_platform/testing/rates_EOD_test.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results)
    rates_df = create_EOD_dataframe(rates, start_date='2022-03-02')
    
    # Process rates
    new_df = process_EOD_rates(rates_df)

    # Verify the first symbol
    assert new_df.symbol[0] == 'AED'
    assert new_df.rate[0] == 4.082038


def test_extractRates_LoadToBigQuery():

    # Initialize values
    with open('/projects/stock_analysis_platform/testing/rates_EOD_test.txt', 'r') as read_file:
        results = read_file.read()
        read_file.close()

    rates = extract_rates_dictionary(results)
    rates_df = create_EOD_dataframe(rates, start_date='2022-03-02')
    new_df = process_EOD_rates(rates_df)


    load_to_bigquery_EOD(start_date='2022-03-02')