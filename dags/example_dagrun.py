from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def stampa_parametri(**kwargs):
    conf =kwargs['dag_run'].conf if 'dag_run' in kwargs else {}  # Ottieni i parametri passati da dagrun.cfg
    message = conf.get('message', 'Messaggio di default')
    number = conf.get('number', 0)
    print(f"Messaggio ricevuto: {message}")
    print(f"Numero ricevuto: {number}")

# Definizione del DAG
with DAG(
    dag_id="example_dag",
    schedule_interval=None,  # Esegui manualmente
    start_date=days_ago(1),
    catchup=False,
    tags = ['test']
) as dag:
    
    task_stampa = PythonOperator(
        task_id="stampa_parametri",
        python_callable=stampa_parametri,
        provide_context=True  # Passa il contesto Airflow alla funzione
    )

    task_stampa  # Definisce la sequenza del DAG

