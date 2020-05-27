from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable as v
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'KP',
    'start_date': datetime(2020, 5, 24, 20, 0, 0),
    'end_date': datetime(2020, 5, 24, 22, 0 , 0),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

dag = DAG('dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='public.staging_events',
    redshift_conn_id='redshift',    
    redshift_iam_role=v.get('redshift_iam_role'),
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    json_location='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='public.staging_songs',
    redshift_conn_id='redshift',    
    redshift_iam_role=v.get('redshift_iam_role'),
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    region='us-west-2',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songplays', 
    truncate=True,
    sub_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.users', 
    truncate=True,
    sub_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songs', 
    truncate=True,
    sub_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.artists', 
    truncate=True,
    sub_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.time', 
    truncate=True,
    sub_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables = [  'public.staging_events',
                'public.staging_songs',
                'public.songplays',
                'public.users',
                'public.artists',
                'public.songs',
                'public.time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



# Task dependencies:
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 

load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table, 
                         load_time_dimension_table]

[load_user_dimension_table, 
 load_song_dimension_table, 
 load_artist_dimension_table, 
 load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator