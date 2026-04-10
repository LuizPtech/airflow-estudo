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
SOURCE_BUCKET = 'pasta1'
DESTINATION_BUCKET = 'pasta2'
USERS_SOURCE_OBJECT = "mongodb-atlas/airbnb/*.parquet"
USERS_DESTINATION_OBJECT = "mongodb-atlas/airbnb/users/*.parquet"


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

    # TODO task dependences

    start >> end

    # TODO DAG Instantiation

dag = init()

