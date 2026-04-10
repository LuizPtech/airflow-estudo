import requests
import logging
from datetime import datetime

from airflow  import DAG
from airflow.operators.python import PythonOperator


API = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

# TODO extract
def extract_bitcoin():
    return requests.get(API).json()

# TODO process
def process_bitcoin(ti):
    response = ti.xcom_pull(task_ids="extract_bitcoin")
    logging.info(response)
    processed_data = {"BTCUSD": response["price"]} # Eu passo a chave do JSON e oque é para puxar, nesse caso seria o preço
    ti.xcom_push(key="processed_data", value=processed_data)


# TODO store
def store_bitcoin(ti):
    data = ti.xcom_pull(task_ids="processed_data", key="processed_data" )
    logging.info(data)

with DAG (
    dag_id = "cls-bitcoin",
    schedule = "@daily",
    start_date = datetime(2021, 12, 1),
    catchup = False

):
    # TODO Task 1
    extract_bitcoin_from_api = PythonOperator(
        task_id = "extract_bitcoin",
        python_callable=extract_bitcoin
    )
    # TODO Task 2
    process_bitcoin_from_api = PythonOperator(
        task_id = "process_bitcoin",
        python_callable=process_bitcoin
    )
    # TODO Task 3
    store_bitcoin_in_database = PythonOperator(
        task_id = "store_bitcoin",
        python_callable=store_bitcoin
    )

# TODO set dependencies
extract_bitcoin_from_api >> process_bitcoin_from_api >> store_bitcoin_in_database

