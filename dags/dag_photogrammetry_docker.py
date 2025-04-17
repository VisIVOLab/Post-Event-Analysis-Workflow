from airflow import DAG
import os, sys
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException
from pathlib import Path
from dotenv import load_dotenv
from docker.types import DeviceRequest
from airflow.operators.bash import BashOperator

load_dotenv()
metashapelicense = os.getenv("AGISOFT_FLS")

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

from src.dynamic_mount_docker_operator import DynamicMountDockerOperator

class PhotogrammetryDinamicDocker (DynamicMountDockerOperator):

    def __init__(self, task_id: str, command: str, container_name: str = "metashape-slim-container", **kwargs):
        super().__init__(
            task_id=task_id,
            image='metashape-slim',
            container_name=container_name,
            command=command,
            api_version='auto',
            auto_remove='force',
            docker_url='unix://var/run/docker.sock',
            network_mode='host',
            mount_tmp_dir=False,
            input_path="{{ dag_run.conf.get('settings', {}).get('image_path') }}",
            target_input_path="/photogrammetry/images",
            output_path="{{ dag_run.conf.get('settings', {}).get('project_path') }}",
            target_output_path="/photogrammetry/project",
            working_dir="/home/photogrammetry/src",
            src_path=f"{project_root_folder}/docker",
            target_src_path="/home/photogrammetry/src",
            do_xcom_push=False,
            #user=f"{os.getuid()}",
            environment={
                'AGISOFT_FLS':metashapelicense
            },
            device_requests=[
                DeviceRequest(count=-1, capabilities=[['gpu']])
            ],
            **kwargs
        )

# Default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 21),
    'retries': 0
}

dag = DAG(
    dag_id='dag_photogrammetry_depth_map_docker',
    default_args=default_args,
    schedule_interval=None,  # Manual start
    catchup=False, # By default, it is set to True, and it will execute the script based on the schedule interval from that day to today (monthly/daily, etc.)
    # Airflow will ignore missing dates and will only execute the next scheduled run
    tags= ['dagrun', 'depth map', 'docker']
)

# Task definition
task_docker_init = PhotogrammetryDinamicDocker(
    task_id="data_initialise",
    command= "python step_photogrammetry/init_config.py ",
    dag=dag
)

task_docker_new = PhotogrammetryDinamicDocker(
    task_id="new_project",
    command= "python step_photogrammetry/new_project.py ",
    dag=dag
)

task_docker_import_photos = PhotogrammetryDinamicDocker(
    task_id="import_photos",
    command= "python step_photogrammetry/import_photos.py ",
    dag=dag
)

task_docker_match = PhotogrammetryDinamicDocker(
    task_id="match_align",
    command= "python step_photogrammetry/match_align_photos.py ",
    dag=dag
)

task_docker_depth = PhotogrammetryDinamicDocker(
    task_id="depth_map",
    command= "python step_photogrammetry/build_depth_maps.py ",
    dag=dag
)

task_docker_point = PhotogrammetryDinamicDocker(
    task_id="point_cloud",
    command= "python step_photogrammetry/build_point_cloud.py ",
    dag=dag
)  

task_docker_model = PhotogrammetryDinamicDocker(
    task_id="3D_model",
    command= "python step_photogrammetry/build_model.py",
    dag=dag
)

task_docker_init >> task_docker_new >> task_docker_import_photos >> task_docker_match >> task_docker_depth >>  task_docker_model