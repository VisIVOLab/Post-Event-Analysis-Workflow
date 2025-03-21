from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

def load_images(image_folder, **kwargs):
    files = [f for f in os.listdir(image_folder) if f.endswith(('JPG','jpg', 'jpeg', 'tif', 'tiff'))]
    print(f"Found {len(files)} images in {image_folder}")
    return files

def process_metashape(image_folder, output_folder, **kwargs):
    script_path = "metashape_general_workflow.py"
    command = ["python", script_path, image_folder, output_folder]
    subprocess.run(command, check=True)

def export_results(output_folder, **kwargs):
    files = os.listdir(output_folder)
    print(f"Exported files: {files}")

# Configurazione DAG
default_args = {
    'owner': 'leonardo',
    'start_date': datetime(2025, 3, 20),
    'retries': 1,
}

dag = DAG(
    dag_id='test_full_metashape_processing_V3',
    default_args=default_args,
    description='DAG per il processo fotogrammetrico con Metashape',
    schedule_interval=None,  # Esegui manualmente o imposta un orario
)

task_load = PythonOperator(
    task_id='load_images',
    python_callable=load_images,
    op_kwargs={'image_folder': '/home/leonardo/AirflowDemo/metashape_input'},
    dag=dag,
)

task_process = PythonOperator(
    task_id='process_metashape',
    python_callable=process_metashape,
    op_kwargs={'image_folder': '/home/leonardo/AirflowDemo/metashape_input', 'output_folder': '/home/leonardo/AirflowDemo/metashape_output'},
    dag=dag,
)

task_export = PythonOperator(
    task_id='export_results',
    python_callable=export_results,
    op_kwargs={'output_folder': '/home/leonardo/AirflowDemo/metashape_output'},
    dag=dag,
)

task_load >> task_process >> task_export

"""
Gestione dipendenze: possiamo suddividerlo in più task (es. caricamento immagini, elaborazione, esportazione risultati).
Creeremo un DAG con tre task principali:

    Caricamento immagini – per spostare o preparare le immagini da elaborare.
    Elaborazione con Metashape – il core del tuo processo.
    Esportazione dei risultati – per salvare i dati finali dove servono.

Ho analizzato il tuo script. Lo suddivideremo in tre task nel DAG di Airflow:

    Caricamento immagini: trova e carica le immagini nella cartella.
    Elaborazione con Metashape: esegue il workflow (match, align, depth maps, modello, UV, texture, point cloud, DEM, ortomosaico).
    Esportazione risultati: salva report, modello, point cloud, DEM e ortomosaico.

Aggiorna i percorsi (/path/to/images, /path/to/output, /path/to/general_workflow.py) e sarà pronto all'uso!
"""