from airflow import DAG
import os, sys
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import TaskInstance
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

from src.tasks.dag_configuration import set_configuration_env
from src.tasks.new_project import new_project
from src.tasks.import_photos import import_photos
from src.tasks.match_align_photos import match_and_align
from src.tasks.build_depth_maps import build_depth_maps
from src.tasks.build_point_cloud import build_point_cloud
from src.tasks.build_model import build_model
from src.tasks.build_tiled import build_tiled

def export_cloud(**kwargs):
    import Metashape

    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')
    Metashape.app.cpu_enable = ti.xcom_pull(task_ids='data_initialise', key='cpu_enable')
    Metashape.app.gpu_mask = ti.xcom_pull(task_ids='data_initialise', key='gpu_mask')

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=True)
    chunk = doc.chunks[0]

    chunk.exportPointCloud(os.path.join(output_folder, 'point_cloud.las'))
    print("Export point cloud")

# Default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': False, # Today's task will only start if yesterday's task has been completed successfully.
    'start_date': datetime(2025, 3, 21),
    'retries': 3,
}

dag = DAG(
    dag_id='dag_photogrammetry_point_cloud_dagrun_v10',
    default_args=default_args,
    schedule_interval=None,  # Manual Start
    catchup=False, # By default, it is set to True, and it will execute the script based on the schedule interval from that day to today (monthly/daily, etc.)
    # Airflow will ignore missing dates and will only execute the next scheduled run
    tags= ['dagrun', 'point cloud', 'no save']
)

# Task definition
task_data_initialise = PythonOperator(
    task_id="data_initialise",
    python_callable=set_configuration_env,
    provide_context=True
)

task_new_project = PythonOperator(
    task_id='new_project',
    python_callable=new_project,
    provide_context=True,
    dag=dag
)

task_import_photos = PythonOperator(
    task_id='import_photos',
    python_callable=import_photos,
    provide_context=True,
    dag=dag
)

task_match_and_align = PythonOperator(
    task_id='match_and_align',
    python_callable=match_and_align,
    provide_context=True,
    dag=dag
)

task_build_depth_maps = PythonOperator(
    task_id='build_depth_maps',
    python_callable=build_depth_maps,
    provide_context=True,
    dag=dag
)

task_build_point_cloud = PythonOperator(
    task_id='build_point_cloud',
    python_callable=build_point_cloud,
    provide_context=True,
    dag=dag
)

task_build_model = PythonOperator(
    task_id='build_model',
    python_callable=build_model,
    provide_context=True,
    dag=dag
)

task_build_tiled = PythonOperator(
    task_id='build_tiled',
    python_callable=build_tiled,
    provide_context=True,
    dag=dag
)

task_export_cloud = PythonOperator(
    task_id='export_cloud',
    python_callable=export_cloud,
    provide_context=True,
    dag=dag
)

task_data_initialise >> task_new_project >> task_import_photos >> task_match_and_align >> task_build_depth_maps >> task_build_point_cloud >> [task_export_cloud, task_build_tiled, task_build_model]
