from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Operator that handles copying data from S3 to a staging table in redshift
"""
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_from_s3 = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        compupdate off 
        region '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 s3_jsonpath_file="",
                 s3_region="",
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.s3_jsonpath_file=s3_jsonpath_file
        self.s3_region=s3_region
        self.redshift_conn_id=redshift_conn_id
        self.table=table   

    def execute(self, context):
        self.log.info('StageToRedshiftOperator init')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Delete data from staging table for populating it with new data')
        redshift_hook.run("DELETE FROM {}".format(self.table))
        
        combined_s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        
        json_path = self.s3_jsonpath_file
        if json_path != 'auto':
            json_path = f's3://{self.s3_bucket}/{json_path}'
            
        self.log.info('Copying data from S3 to staging table...')
        redshift_hook.run(StageToRedshiftOperator.copy_from_s3.format(
            self.table,
            combined_s3_path,
            credentials.access_key, 
            credentials.secret_key,
            self.s3_region,
            json_path
        ))





