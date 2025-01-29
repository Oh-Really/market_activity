from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from datetime import datetime, timedelta, date
import requests
import pandas as pd
import pendulum


url = 'https://analytics.trayport.com/api/trades/activity'
today = date.today()
headers = {
    'x-api-key': Variable.get('api_key'),
    'accept': 'application/json'
}
paramaters = {
    'from' : today.strftime('%Y-%m-%dT%H:%M:%S')
}


def get(url, params, headers):
    response = requests.get(url, params, headers=headers)
    if response.status_code != 200:
        raise Exception(f'Status: {response.status_code}. {response.content.decode()}')
    return response.content.decode("utf-8")

#@task(task_id="print_the_context")
def get_df(url, params={}, headers=headers):
    content = get(url, params, headers)
    df = pd.read_json(content)
    print(df)


# Default DAG arguments
default_args = {
    'owner': 'airflow',
}

#Define the DAG
# with DAG(
#     'fetch_api_data',
#     default_args=default_args,
#     description='Fetch data from the analytics API and store in Postgres',
#     catchup=False,
# ) as dag:
#     fetch_task = PythonOperator(
#         task_id='analytics_api_call',
#         python_callable=get_df,
#         op_kwargs = {'url' : url, 'params' : paramaters, 'headers': headers }
#     )