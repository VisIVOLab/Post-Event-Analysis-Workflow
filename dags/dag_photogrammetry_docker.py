from airflow import DAG
import os, sys
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

from src.tasks.dag_configuration import set_configuration_env
from src.dynamic_mount_docker_operator import DynamicMountDockerOperator

class PhotogrammetryDinamicDocker (DynamicMountDockerOperator):

    def __init__(self, task_id: str, command: str, container_name: str = "metashape-slim-container", **kwargs):
        super().__init__(
            task_id=task_id,
            image='metashape-slim',
            container_name=container_name,
            command=command,
            api_version='auto',
            auto_remove="force",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mount_tmp_dir=False,
            input_path="{{dag_run.conf.settings['image_path']}}",
            target_input_path="/photogrammetry/images",
            output_path="{{dag_run.conf.settings['project_path']}}",
            target_output_path="/photogrammetry/project",
            src_path=f"{project_root_folder}/src",
            target_src_path="/photogrammetry/src",
            user=f"{os.getuid()}",
            **kwargs
        )

# Default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': False, # Today's task will only start if yesterday's task has been completed successfully.
    'start_date': datetime(2025, 3, 21),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_photogrammetry_depth_map_docker_v1',
    default_args=default_args,
    schedule_interval=None,  # Manual start
    catchup=False, # By default, it is set to True, and it will execute the script based on the schedule interval from that day to today (monthly/daily, etc.)
    # Airflow will ignore missing dates and will only execute the next scheduled run
    tags= ['dagrun', 'depth map', 'docker']
)

#install_task = install_dependencies_task(dag)

# Task definition
task_docker_one = PhotogrammetryDinamicDocker(
    task_id="data_initialise_1",
    command= """echo test""",
    dag=dag
)

task_docker_two = PhotogrammetryDinamicDocker(
    task_id="data_initialise_2",
    command= """echo test 2""",
    dag=dag
)

""" task_data_initialise = PythonOperator(
    task_id="data_initialise",
    python_callable=set_configuration_env,
    provide_context=True
) 
"""

task_docker_one >> task_docker_two