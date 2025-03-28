from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import time
from pathlib import Path


default_args = {
    'owner': 'mauro',
    'folder': '/home/mauro/projects/AirflowDemo/data'
}

dag = DAG(
    dag_id='mauro_test',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False, # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
    tags = ['mauro', 'test']
)

def start(**kwargs):
    data = {
        "step" :[{
            "id": 1,
            "status": "ok"
        }]
    }
    json_path = Path(default_args['folder']) / 'project.json'
    with open(json_path, 'w') as f:
        json.dump(data, f, indent=4)
    ti = kwargs['ti']
    ti.xcom_push(key='counter', value=0)

task_start = PythonOperator(
    task_id='start',
    python_callable=start,
    dag=dag
)

def end():  
    print(f"task_end")
    return True
task_end = PythonOperator(
    task_id='end',
    python_callable=end,
    dag=dag
)


def init_ml_flag(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='ml_flag', value=True)
task_init_ml_flag = PythonOperator(
    task_id='init_ml_flag',
    python_callable=init_ml_flag,
    dag=dag
)

def ml_true(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='ml_flag', value=True)
    print(f"ml_true")

def ml_false(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='ml_flag', value=False)
    print(f"ml_false")

def read_ml_flag(**kwargs):  
    ti = kwargs['ti']
    ml_flag = ti.xcom_pull(key='ml_flag', task_ids='init_ml_flag')
    print(f"read  ml_flag: {ml_flag}")
    return ml_flag


def mock(**kwargs):
    time.sleep(5)
    ml_false(**kwargs)
    return True

task_mock1 = PythonOperator(
    task_id='mock1',
    python_callable=mock,
    dag=dag
)

task_mock2 = PythonOperator(        
    task_id='mock2',
    python_callable=mock,
    dag=dag
)

def ml(**kwargs):
    # check della memoria della scheda video
    # controlla il formato delle immagini
        # controlla che il formato sia lo stesso  
    # calcola per eccesso la memoria occupata dalla rete per una immagine
    # confronta i valori e determina (in modo conservativo) il massimo bach_size
    # carica il corrispondente dataloader

    # ciclo sui batches
    while(True):
        # per ogni batch
            # controlla il via libera
        # ml_flag = read_ml_flag(**kwargs)
        ti = kwargs['ti']
        ml_flag = ti.xcom_pull(key='ml_flag', task_ids='mock1')
        print(f"task_ml    ml_flag: {ml_flag}")    
        if not ml_flag:
            break
        else:
            time.sleep(1)

                # se TRUE
                    # calcola le maschere per il batch
                    # salva le maschere in png
                # se FALSE
                    # interrompi il processo ed esci
    # setta il via libera a TRUE (per la prossima esecuzione)

    ml_true(**kwargs)
    return True

task_ml = PythonOperator(
    task_id='ml',
    python_callable=ml,
    dag=dag
)

task_init_ml_flag >> task_start

task_start >> task_mock1 
task_start >> task_ml 

task_mock2 << task_ml
task_mock2 << task_mock1

task_end << task_mock2





