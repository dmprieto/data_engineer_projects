from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateSparkifyTablesOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'dprieto',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 1, 1, 0, 0, 0),
    'end_date': datetime(2018, 6, 1, 3, 0, 0, 0),
    'retries':2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow - Sparkify',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_sparkify_tables = CreateSparkifyTablesOperator(
    task_id='create_sparkify_tables',
    dag=dag,
    postgres_conn_id="redshift",
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    s3_jsonpath_file="log_json_path.json",
    s3_region='us-west-2',
    table="staging_events",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_jsonpath_file="auto",
    s3_region='us-west-2',
    table="staging_songs",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_load_query=SqlQueries.songplays_table_insert,
    provide_context=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_load_query=SqlQueries.users_table_insert,
    table="users",
    delete_load=True,
    provide_context=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_load_query=SqlQueries.songs_table_insert,
    table="songs",
    provide_context=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_load_query=SqlQueries.artists_table_insert,
    table="artists",
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_load_query=SqlQueries.time_table_insert,
    table="time",
    provide_context=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays','users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_sparkify_tables
create_sparkify_tables >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks 
run_quality_checks >> end_operator
