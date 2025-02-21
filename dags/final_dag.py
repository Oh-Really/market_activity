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

    fetch_instrument_data_df = DataFetchOperator(task_id='instruments_api_call', endpoint='instruments')

    fetch_trades_data_df = DataFetchOperator(task_id='trades_api_call', endpoint='trades')

    create_postgres_table = CreateTables(task_id='create_table', conn_id='my_postgres')

    @task()
    def load_df_to_postgres(df_list: list[pd.DataFrame]):
        postgres = PostgresHook(postgres_conn_id='my_postgres')
        conn_str = postgres.get_uri()
        engine = create_engine(conn_str)
        for idx, df in enumerate(df_list):
            if idx == 0:
                df.to_sql('instruments', engine, if_exists='replace', index=False)
            else:
                df.to_sql('trades', engine, if_exists='replace', index=False)

    #TODO create task to run app.py in streamlit folder   
    
    
    
    #TODO group tasks using @task_group
    

    
    # Define dependencies
    start_operator >> fetch_instrument_data_df
    fetch_instrument_data_df >> fetch_trades_data_df
    fetch_trades_data_df >> create_postgres_table
    create_postgres_table >> load_df_to_postgres([fetch_instrument_data_df.output, fetch_trades_data_df.output])

fetch_data_dag = trade_project()