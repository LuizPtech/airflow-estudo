"""
DAG Skeleton

- Imports
- Connections e Variables
- Default Arguments
- DAG Definition
- Task Declaration
- Task Dependencies
- DAG Instantiation

"""

# TODO IMPORTS
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# TODO Connections & Variables

GCS_CONN_ID = 'gcp-bucket'
SOURCE_BUCKET = 'estudo-airflow-teste'
DESTINATION_BUCKET = 'estudo-airflow-teste'
USERS_SOURCE_OBJECT = "pasta1/airbnb/listings/*.parquet"
USERS_DESTINATION_OBJECT = "pasta2"



# TODO Default arguments
default_args = {
    'owner': 'luiz phelipe m. goncalves',
    'retries': 1,
    'retry_relay': timedelta(minutes=5)
}

# TODO DAG definition

@dag(
    dag_id= 'dag-first-operator',
    start_date= datetime(2024,9,30),
    max_active_runs=1,
    schedule_interval= timedelta(minutes=5),
    catchup=False,
    default_args=default_args,
    tags=['developement', 'elt', 'gcs', 'files']

)
def init():

    # TODO task declaration


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    copy_parquet_aibnb_to_gcs = GCSToGCSOperator(
        task_id='copy_parquet_aibnb_to_gcs',
        source_bucket=SOURCE_BUCKET,
        source_object=USERS_SOURCE_OBJECT,
        destination_bucket=DESTINATION_BUCKET,
        destination_object=USERS_DESTINATION_OBJECT,
        move_object=False,
        gcp_conn_id=GCS_CONN_ID,
    )


    # TODO task dependences

    start >> copy_parquet_aibnb_to_gcs >> end

    # TODO DAG Instantiation

dag = init()

