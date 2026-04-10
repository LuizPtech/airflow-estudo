"""
Traditional Modeler

- Instantiate DAG Class
- Verbose & Explicit
- Manual Pushing & Pulling of XComs
- Harder to Use


"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def my_task():
    print("Hellow from Tradicional DAG")

dag = DAG('traditional_dag', start_date=datetime(2024,1,1))

task = PythonOperator(task_id='my_task', python_callable=my_task, dag=dag)

