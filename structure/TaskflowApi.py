"""
TaskFlowAPI

-   Python way of Building Tasks
-   Use of Decorators
-   Values from Tasks Automatically
-   Better Maintainability
"""

from airflow.decorators import task, dag
from datetime import datetime, timedelta


@dag(start_date=datetime(2024, 1, 1))
def taskflow_dag():
    @task
    def my_task():
        print("Hellow from TaskFlow API!")

    my_task()

dag=taskflow_dag()