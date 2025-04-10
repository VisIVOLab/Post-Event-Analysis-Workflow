from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable


from datetime import datetime
import json
import time
import os
import sys
from pathlib import Path


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))



default_args = {
    'owner': 'mauro',
    'repo_folder': project_root_folder / 'ml' ,
    'repo_url': f"https://github.com/ICSC-Spoke3/HaMMon-ML-digital-twin.git"
}

data_folder = project_root_folder / 'data'

dag = DAG(
    dag_id='ml_env_setup',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False, # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
    tags = ['mauro', 'ml']
)


# Task 1: Clone or update the ML repository
setup_ml_repo = BashOperator(
    task_id='setup_ml_repo',
    bash_command=f"""
        REPO_BASE_DIR="{default_args['repo_folder']}"
        REPO_NAME="HaMMon-ML-digital-twin"
        REPO_URL="{default_args['repo_url']}"
        TARGET_DIR="$REPO_BASE_DIR/$REPO_NAME"

        mkdir -p "$REPO_BASE_DIR"

        if [ -d "$TARGET_DIR/.git" ]; then
            echo "Repo already exists. Pulling latest changes..."
            cd "$TARGET_DIR"
            git pull
        else
            echo "Cloning repo for the first time..."
            git clone --depth 1 "$REPO_URL" "$TARGET_DIR"
        fi
    """,
    dag=dag
)

# Task 2: Install the requirements from the ML repository
install_requirements = BashOperator(
    task_id='install_ml_requirements',
    bash_command=f"""
        pip install -r {default_args['repo_folder']}/HaMMon-ML-digital-twin/requirements.txt
    """,
    dag=dag
)


# Task chaining
setup_ml_repo >> install_requirements

