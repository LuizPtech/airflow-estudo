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
import json
# TODO LIBRARIES

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk.definitions.decorators import task

# TODO Connections & Variables

mongodb_atlas_conn_id = 'mongodb_default'

# TODO Default arguments
default_args = {
    'owner': 'luiz phelipe m. goncalves',
    'retries': 1,
    'retry_relay': timedelta(minutes=5)
}

# TODO DAG definition

@dag(
    dag_id= 'dag-structure-hook-mongodb',
    start_date= datetime(2024,9,30),
    max_active_runs=1,
    schedule_interval= timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=['first', 'dag']

)
def init():
    # TODO task declaration


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # TODO Retrieve users collection from MongoDB

    @task
    def get_users():
        hook = MongoHook(conn_id = mongodb_atlas_conn_id)
        database = "sample_airbnb"
        collections = "listingsAndReviews"

        query = { "property_type":"House" }

        try:
            conn = hook.get_conn()
            db = conn[database]
            coll = db[collections]

            count = coll.count_documents(query)

            data = hook.find(
                mongo_db=database,
                mongo_collection=collections,
                query=query
            )

            documents = list(data)
            doc_count = len(documents)

            if doc_count == 0:
                print("No documents found")
                return None

            elif doc_count > 1:
                print(f"Warning: Found`{doc_count}` documents")


            documents = json.loads(json.dumps(documents[0], default=str))
            print(f"Retrieved document:{documents}")

            return documents

        except Exception as e:
            print(f"Error: {str(e)}")
            raise
    # TODO task dependences

    users_collection = get_users()
    start >> users_collection >>end

    # TODO DAG Instantiation

dag = init()

