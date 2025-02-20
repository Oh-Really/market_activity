from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class CreateTables(BaseOperator):

    sql_create= """
            CREATE TABLE IF NOT EXISTS trades (
            instrumentId INT,
            sequenceId INT,
            sequenceItemId INT,
            secondSequenceItemId INT,
            count INT
        );
        """
    

    @apply_defaults
    def __init__(self, conn_id='', *args, **kwargs):
        super(CreateTables, self).__init__(*args, **kwargs)
        self.conn_id = conn_id

    
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Creating trades table in separate postgres")
        postgres.run(CreateTables.sql_create)