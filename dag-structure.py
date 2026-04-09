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

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


# TODO Default arguments
default_args = {
    'owner': 'luiz phelipe m. goncalves',
    'retries': 1,
    'retry_relay': timedelta(minutes=5)
}

# TODO DAG definition

@dag(
    dag_id= 'dag-structure',
    start_date= datetime(2024,9,30),
    max_active_runs=1,
    schedule_interval= timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=['first', 'dag']

)
def init():
    # TODO task declaration


    start = EmptyOperators(task_id='start')
    end = EmptyOperators(task_id='end')

    # TODO task dependences

    start >> end

    # TODO DAG Instantiation

dag = init()

