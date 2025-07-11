from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

def say_hello():
    print("Hello from PythonOperator!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello_python = PythonOperator(
        task_id="hello_python",
        python_callable=say_hello,
    )

    hello_bash = BashOperator(
        task_id="hello_bash",
        bash_command="echo 'Hello from BashOperator!'",
    )

    start >> hello_python >> hello_bash
