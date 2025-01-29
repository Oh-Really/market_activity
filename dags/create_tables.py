import pendulum

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


class CreateTables(BaseOperator):

    sql= """
            CREATE TABLE IF NOT EXISTS trades (
            instrumentId INT,
            sequenceId INT,
            sequenceItemId INT,
            secondSequenceItemId INT,
            count INT
        );
        """
    

    @apply_defaults
    def __init__(self, 
                 conn_id='',
                 *args,
                 **kwargs):
        super(CreateTables, self).__init__(*args, **kwargs)
        self.conn_id = conn_id

    
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Creating trades table in separate postgres")
        postgres.run(CreateTables.sql)
        



# @dag(start_date = pendulum.now())
# def create_table():
#     create_table_task = SQLExecuteQueryOperator(
#     task_id = "create_table",
#     conn_id = 'my_postgres',
#     sql= """
#             CREATE TABLE IF NOT EXISTS trades (
#             instrumentId INT,
#             sequenceId INT,
#             sequenceItemId INT,
#             secondSequenceItemId INT,
#             count INT,
#         );
#         """)
    
# create_table_dag = create_table()