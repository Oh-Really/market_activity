from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.load_dimension import DataFetchOperator
#import sys
#import os
#sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from load_dimension import DataFetchOperator
from create_tables import CreateTables
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

    fetch_api_data = DataFetchOperator(task_id='analytics_api_call')

    create_postgres_table = CreateTables(task_id='create_table', conn_id='my_postgres')

    #@task()
    #def load_df_to_postgres(df: pd.df):
        #postgres = PostgresHook(postgres_conn_id='my_postgres')
        #postgres.run('INSERT INTO trades)

    
    # Define dependencies
    start_operator >> fetch_api_data
    fetch_api_data >> create_postgres_table

fetch_data_dag = trade_project()