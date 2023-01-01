import os
from google.cloud import bigquery

###              ###
### App Related  ###
###              ###

def get_rates():

    # Setting Google Credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/projects/stock_analysis_platform/dags/ServiceKey_GoogleCloud.json'

    # Creating a BigQuery client
    bigquery_client = bigquery.Client()

    # Fetching the data from BigQuery
    query = "SELECT * FROM Forex_Platform.rates order by date, symbol;"

    # Retrieving the results from BigQuery
    query_job = bigquery_client.query(query)
    results = query_job.result()

    # Creating a Pandas DataFrame
    df = results.to_dataframe()
    
    return df