from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import time
import os
import sys
from pathlib import Path


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

from src.utils import Flag


default_args = {
    'owner': 'mauro',
    'data_folder': project_root_folder / 'data'
}



dag = DAG(
    dag_id='mauro_test',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False, # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
    tags = ['mauro', 'test']
)

ml_flag = Flag('ml', True)

def start(**kwargs):
    data = {
        "step" :[{
            "id": 1,
            "status": "ok"
        }]
    }
    json_path = Path(default_args['data_folder'])/ 'project.json'
    with open(json_path, 'w') as f:
        json.dump(data, f, indent=4)
    ti = kwargs['ti']
    ti.xcom_push(key='counter', value=0)
    print(f'cwd_ {os.getcwd()}')

    print("DAG folder:", dag_folder)
    ml_flag.true()

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


def mock(**kwargs):
    time.sleep(5)
    ml_flag.false()
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
        flag = ml_flag.get()
        print(f"task_ml    ml_flag: {flag}")    
        if not flag:
            break
        else:
            time.sleep(1)

                # se TRUE
                    # calcola le maschere per il batch
                    # salva le maschere in png
                # se FALSE
                    # interrompi il processo ed esci
    # setta il via libera a TRUE (per la prossima esecuzione)

    ml_flag.true()
    return True

task_ml = PythonOperator(
    task_id='ml',
    python_callable=ml,
    dag=dag
)


task_start >> task_mock1 
task_start >> task_ml 

task_mock2 << task_ml
task_mock2 << task_mock1

task_end << task_mock2





