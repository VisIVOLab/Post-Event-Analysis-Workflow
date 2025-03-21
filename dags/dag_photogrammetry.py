from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
""" from task.align_cameras import align_cameras
from task.build_point_cloud import build_point_cloud
from task.build_mesh import build_mesh
from task.build_texture import build_texture
from task.export_results import export_results """

def new_project(output_folder):
    import Metashape
    import os, logging

    """Apre o crea un progetto Metashape."""
    print("New Project")
    # Checking compatibility
    compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version))

    doc = Metashape.Document()
    project_path = os.path.join(output_folder, "project.psx")
    """ if os.path.exists(project_path):
        doc.open(project_path)
        print("Old Project")
    else:
        doc.save(project_path)
        print("New Project") """
    doc.save(project_path)
    logging.info("🚀 progetto creato!")

def import_photos(image_folder, output_folder):
    import Metashape
    import logging, os
    print("Import Photos")
    photos = [entry.path for entry in os.scandir(image_folder) if entry.is_file() and entry.name.lower().endswith(('.jpg', '.jpeg', '.tif', '.tiff'))]
    
    doc = Metashape.app.document
    project_path = os.path.join(output_folder, "project.psx")
    doc.open(project_path)
    chunk = doc.addChunk()  # Aggiunge un nuovo chunk al progetto
    chunk.addPhotos(photos)
    doc.save()
    
    print(f"{len(chunk.cameras)} images loaded.")
    logging.info(f"🚀 {len(chunk.cameras)} images loaded.")


# Definizione degli argomenti di default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': True, # Il task di oggi partirà solo se quello di ieri è stato completato con successo.
    'start_date': datetime(2025, 3, 21),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_photogrammetry_v4',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
)

# Definizione dei task

t1 = PythonOperator(
    task_id='new_project',
    python_callable=new_project,
    op_kwargs={'output_folder': '/home/leonardo/AirflowDemo/metashape_output'},
    dag=dag
)

t2 = PythonOperator(
    task_id='import_photos',
    python_callable=import_photos,
    op_kwargs={'image_folder': '/home/leonardo/AirflowDemo/metashape_input', 'output_folder': '/home/leonardo/AirflowDemo/metashape_output'},
    dag=dag
)

""" t2 = PythonOperator(
    task_id='align_cameras',
    python_callable=align_cameras,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t3 = PythonOperator(
    task_id='build_point_cloud',
    python_callable=build_point_cloud,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t4 = PythonOperator(
    task_id='build_mesh',
    python_callable=build_mesh,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t5 = PythonOperator(
    task_id='build_texture',
    python_callable=build_texture,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t6 = PythonOperator(
    task_id='export_results',
    python_callable=export_results,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
) """

# Definizione delle dipendenze
t1 #>> t2 #>> t3 >> t4 >> t5 >> t6
