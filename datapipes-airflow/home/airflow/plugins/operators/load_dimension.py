from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Operator that handles loading a dimension tables in Sparkify
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_load_query="",
                 table="",
                 delete_load="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_load_query = sql_load_query
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('Loading Sparkify dimension table '+ self.table +' with data ...')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_load:
            self.log.info(f"Deleting data from dimension table {self.table} before loading new data")
            redshift_hook.run("DELETE FROM {}".format(self.table))
        
        redshift_hook.run(self.sql_load_query)
