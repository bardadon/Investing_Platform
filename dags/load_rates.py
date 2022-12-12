from airflow import DAG

from datetime import datetime

default_args = {
    'start_date':datetime(2022,1,1),
    'schedule_interval':'0 0 * * *'
}

with DAG(dag_id='load_rates', default_args= default_args, catchup=False) as dag:

    pass