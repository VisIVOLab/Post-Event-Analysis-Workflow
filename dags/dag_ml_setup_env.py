from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


import os
import sys
from pathlib import Path




dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))


# Default
default_args = {
    'owner': 'Visivo',
    'data_folder': project_root_folder / 'data',
    'ml_folder': project_root_folder / 'ml',
    'repo_folder': project_root_folder / 'ml/repo',
    'repo_url': "https://github.com/ICSC-Spoke3/HaMMon-ML-digital-twin.git",
    'max_label':12,
    "downscale_size": 713
}

labels = [
    ("0", "background"),
#    ("1", "building-flooded"),
    ("2", "building-not-flooded"),
#    ("3", "road-flooded"),
    ("4", "road-not-flooded"),
    ("5", "water"),
    ("6", "tree"),
    ("7", "vehicle"),
#    ("8", "pool"),
    ("9", "grass"),
]

# into the dagrun.cfg
#   "input_folder":  
#   "output_folder"  

dag = DAG(
    dag_id='ml_setup_env',
    default_args=default_args,
    schedule_interval=None, # Manual start
    tags = ['ml']
)


# Clone or update the ML repository
setup_ml_repo = BashOperator(
    task_id='setup_ml_repo',
    bash_command=f"""
        ML_DIR="{default_args['ml_folder']}"
        REPO_DIR="{default_args['repo_folder']}"
        REPO_URL="{default_args['repo_url']}"

        mkdir -p "$ML_DIR"

        if [ -d "$REPO_DIR/.git" ]; then
            echo "Repo already exists. Pulling latest changes..."
            cd "$REPO_DIR"
            git pull
        else
            echo "Cloning repo for the first time..."
            git clone --depth 1 "$REPO_URL" "$REPO_DIR"
        fi
    """,
    dag=dag
)

# Install the requirements from the ML repository
install_requirements = BashOperator(
    task_id='install_ml_requirements',
    bash_command=f"""
        pip install -r {default_args['repo_folder']}/requirements.txt
    """,
    dag=dag
)

setup_ml_repo >> install_requirements