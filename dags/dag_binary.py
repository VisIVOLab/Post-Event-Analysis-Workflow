from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


import os
import sys
from pathlib import Path

# airflow dags trigger ml --conf "$(cat dagrun-ml.cfg)"


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))


# Default
default_args = {
    'owner': 'Visivo',
    'project_root_folder': project_root_folder,
    'data_folder': project_root_folder / 'data',
    'ml_folder': project_root_folder / 'ml',
    'repo_folder': project_root_folder / 'ml/repo',
    'repo_url': "https://github.com/ICSC-Spoke3/HaMMon-ML-digital-twin.git",
    'max_label': 12,
    'downscale_size': 713,
    'output_folder': '/home/mauro/projects/AirflowDemo/data/ml-output',
    'ml_outputs_upscaled': '/home/mauro/projects/AirflowDemo/data/ml-output/tmp_manual__2025-04-11T11:48:40+00:00/ml_outputs_upscaled'
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
    dag_id='binary',
    default_args=default_args,
    schedule_interval=None, # Manual start
    tags = ['ml']
)



prepare_binary_dir = BashOperator(
    task_id="prepare_binary_dir",
    bash_command=f"""
        RUN_ID="{{{{ dag_run.run_id }}}}"
        OUTPUT_FOLDER={ default_args['output_folder']}
        BINARY_DIR="$OUTPUT_FOLDER/binary"
        FINAL_DIR="$BINARY_DIR"

        if [ -d "$BINARY_DIR" ]; then
            echo "Directory 'binary' già esistente. Ne creo una nuova con run_id."
            FINAL_DIR="${{BINARY_DIR}}_${{RUN_ID}}"
            mkdir -p "$FINAL_DIR"
        else
            mkdir -p "$FINAL_DIR"
        fi
    """,
    dag=dag
)


with dag: 
    with TaskGroup("binary_masks_group", tooltip="extract binary masks") as binary_masks_group:
        binary_mask_tasks = []
        for i, (num, label) in enumerate(labels):
            task = BashOperator(
                task_id=f"binary_mask_{i}",
                bash_command=f"""
                    RUN_ID="{{{{ dag_run.run_id }}}}"
                    OUTPUT_FOLDER="{default_args['output_folder']}"
                    UPSCALED_DIR="{default_args['ml_outputs_upscaled']}"
                    BINARY_DIR="$OUTPUT_FOLDER/binary"
                    BINARY_RUN_ID="$OUTPUT_FOLDER/binary_$RUN_ID"

                    if [ -d "$BINARY_RUN_ID" ]; then
                        BINARY_DIR="$BINARY_RUN_ID"
                    fi

                    mkdir -p "$BINARY_DIR/{label}"

                    python {default_args['repo_folder']}/utils/masks_binary.py \
                        "$UPSCALED_DIR" \
                        {num} \
                        "$BINARY_DIR/{label}"
                """,
                dag=dag
            )
            binary_mask_tasks.append(task)
            
for t1, t2 in zip(binary_mask_tasks[:-1], binary_mask_tasks[1:]):
    t1 >> t2




rename_files = BashOperator(
    task_id="rename_files",
    bash_command=f"""
        # Set the output folder from dag_run configuration.
        OUTPUT_FOLDER="{default_args['output_folder']}"
        BINARY_DIR="$OUTPUT_FOLDER/binary"
        
        # If a run-specific binary folder exists, use it.
        if [ -d "$OUTPUT_FOLDER/binary_{{{{ dag_run.run_id }}}}" ]; then
            BINARY_DIR="$OUTPUT_FOLDER/binary_{{{{ dag_run.run_id }}}}"
        fi

        echo "Using BINARY_DIR: $BINARY_DIR"
        bash {default_args['project_root_folder']}/src/scripts/rename_binary_masks.sh "$BINARY_DIR"
    """,
    dag=dag
)



# Task chaining

(
    prepare_binary_dir
    >> binary_masks_group
    >> rename_files
)




