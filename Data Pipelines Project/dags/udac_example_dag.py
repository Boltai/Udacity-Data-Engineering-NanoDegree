from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = ''
song_s3_key = "song_data"
log_s3_key = "log-data"
log_json_file = "log_json_path.json"



default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1        
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table="staging_events",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format="JSON",
    log_json_file = log_json_file,
    redshift_conn_id = "redshift",
    provide_context=True,
    aws_credentials_id="aws_credentials",
    dag=dag
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    s3_bucket = s3_bucket,
    s3_key = song_s3_key,
    file_format="JSON",
    log_json_file = "auto",
    provide_context=True,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table = "songplays",
    sql_query = SqlQueries.songplay_table_insert,
    insert_statement ='(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table = "users",
    delete_check = True,
    sql_query = SqlQueries.user_table_insert,
    insert_statement = '(userid, first_name, last_name, gender, level)',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table = "songs",
    delete_check = True,
    redshift_conn_id="redshift",
    sql_query = SqlQueries.song_table_insert,
    insert_statement = '(songid, title, artistid, year, duration)',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table = "artists",
    delete_check = True,
    redshift_conn_id="redshift",
    sql_query = SqlQueries.artist_table_insert,
    insert_statement = '(artistid, name, location, lattitude, longitude)',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table = "time",
    delete_check = True,
    redshift_conn_id="redshift",
    sql_query = SqlQueries.time_table_insert,
    insert_statement = '(start_time, hour, day, week, month, year, weekday)',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table = ["artists", "songplays", "songs", "time", "users"],
    redshift_conn_id="redshift"
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >>stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
run_quality_checks << load_user_dimension_table
run_quality_checks << load_song_dimension_table
run_quality_checks << load_artist_dimension_table
run_quality_checks << load_time_dimension_table
run_quality_checks >> end_operator 

