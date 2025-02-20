from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
#from airflow.operators.load_dimension import DataFetchOperator
#import sys
#import os
#sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from load_dimension import DataFetchOperator
from create_tables import CreateTables
from sqlalchemy import create_engine
import pandas as pd
#from airflow.plugins.operators.load_dimension import DataFetchOperator



default_args = {
    'owner': 'airflow',
}

@dag(
    default_args=default_args,
    description='Load and display trade activity from today',
    catchup=False
)
def trade_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    fetch_api_data_df = DataFetchOperator(task_id='analytics_api_call')

    create_postgres_table = CreateTables(task_id='create_table', conn_id='my_postgres')

    @task()
    def load_df_to_postgres(df: pd.DataFrame):
        postgres = PostgresHook(postgres_conn_id='my_postgres')
        conn_str = postgres.get_uri()
        engine = create_engine(conn_str)
        df.to_sql('trades', engine, if_exists='replace')

    # Next step: Read data from db back into df
    # Include Streamlit web url for docker compose section
    

    
    # Define dependencies
    start_operator >> fetch_api_data_df
    fetch_api_data_df >> create_postgres_table
    create_postgres_table >> load_df_to_postgres(fetch_api_data_df.output)

fetch_data_dag = trade_project()