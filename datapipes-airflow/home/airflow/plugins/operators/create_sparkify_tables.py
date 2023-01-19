from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import CreateSql

"""
Operator that handles the creation of all the tables in the Sparkify redshift cluster
"""
class CreateSparkifyTablesOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 *args, **kwargs):

        super(CreateSparkifyTablesOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        self.log.info('Creating Sparkify tables...')
        redshift_hook.run(CreateSql.CREATE_STG_EVENTS_TABLE_SQL)
        redshift_hook.run(CreateSql.CREATE_STG_SONGS_TABLE_SQL)
        redshift_hook.run(CreateSql.CREATE_TABLE_SONGSPLAY)
        redshift_hook.run(CreateSql.CREATE_TABLE_USERS)
        redshift_hook.run(CreateSql.CREATE_TABLE_SONGS)
        redshift_hook.run(CreateSql.CREATE_TABLE_ARTISTS)
        redshift_hook.run(CreateSql.CREATE_TABLE_TIME)
