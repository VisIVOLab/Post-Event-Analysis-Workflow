from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'leonardo',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

# Creazione del DAG
with DAG(
    dag_id='our_first_dag',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(datetime.now().year, datetime.now().month, datetime.now().day, 19, 00),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world"
    )

    task1
