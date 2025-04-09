from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


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
    'data_folder': project_root_folder / 'data',
    'img_folder': project_root_folder / 'data/floodnet/img-4000-subset',
    'resize_folder': project_root_folder / 'data/tmp_resize',
    'resize_size': 713,
    'ml_output_folder': project_root_folder / 'data/floodnet/ml-dummy-output',
    'ml_output_upscaled_folder': project_root_folder / 'data/floodnet/ml-dummy-output',
    'ml_output_binary_folder': project_root_folder / 'data/floodnet/ml-dummy-output-binary',
    'upscale_size': 4000,
    'ml_folder': project_root_folder / 'ml',
    'max_label':11,
    'repo_url': "https://github.com/ICSC-Spoke3/HaMMon-ML-digital-twin.git",
    'repo_folder': project_root_folder / 'ml/HaMMon-ML-digital-twin'
}


labels = [
    ("2", "roads"),
    ("3", "vehicles"),
    ("4", "buildings"),
]



dag = DAG(
    dag_id='ml',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False, # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
    tags = ['ml']
)

# Clone or update the ML repository
setup_ml_repo = BashOperator(
    task_id='setup_ml_repo',
    bash_command=f"""
        REPO_BASE_DIR="{default_args['ml_folder']}"
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

# # Install the requirements from the ML repository
# install_requirements = BashOperator(
#     task_id='install_ml_requirements',
#     bash_command=f"""
#         pip install -r {default_args['repo_folder']}/requirements.txt
#     """,
#     dag=dag
# )

# # check and resize input images
# resize_images = BashOperator(
#     task_id='resize_images',
#     bash_command=f"""
#         python {default_args['repo_folder']}/utils/preprocess_resize.py \
#             {default_args['img_folder']} \
#             {default_args['resize_folder']} \
#             {default_args['resize_size']}
#     """,
#     dag=dag
# )

# upscale_masks = BashOperator(
#     task_id='resize_masks',
#     bash_command=f"""
#         python {default_args['repo_folder']}/utils/masks_resize.py \
#             {default_args['ml_output_folder']} \
#             {default_args['ml_output_upscaled_folder']} \
#             {default_args['upscale_size']}
#     """,
#     dag=dag
# )


# binary_masks = BashOperator(
#     task_id='upscale_masks',
#     bash_command=f"""
#         python {default_args['repo_folder']}/utils/masks_resize.py \
#             {default_args['mask_folder']} \
#             {default_args['resized_mask_folder']} \
#             {default_args['resize_size']}
#     """,
#     dag=dag
# )

# validate_masks = BashOperator(
#     task_id='validate_masks',
#     bash_command=f"""
#         python {default_args['repo_folder']}/utils/masks_validate.py \
#             {default_args['ml_output_folder']} \
#             0 {default_args['max_label']} 
#     """,
#     dag=dag
# )


# binary_masks = BashOperator(
#     task_id='binary_masks',
#     bash_command=f"""
#         python {default_args['repo_folder']}/utils/masks_binary.py \
#             {default_args['ml_output_upscaled_folder']} \
#             2 \
#             {default_args['ml_output_binary_folder']}/provola \
#     """,
#     dag=dag
# )

with dag:
    with TaskGroup("binary_masks_group", tooltip="Esegui masks_binary N volte") as binary_masks_group:
        for i, (param1, param2) in enumerate(labels):
            task = BashOperator(
                task_id=f"binary_mask_{i}",
                bash_command=f"""
                    python {default_args['repo_folder']}/utils/masks_binary.py \
                        {default_args['ml_output_upscaled_folder']} \
                        {param1} \
                        {default_args['ml_output_binary_folder']}/{param2}
                """,
                dag=dag
            )



# Task chaining
# setup_ml_repo >> install_requirements >> resize_images >> batch_inference >> upscale_masks >> validate_masks >> binary_masks
# setup_ml_repo >> install_requirements >> resize_images
# setup_ml_repo >> install_requirements >> upscale_masks


setup_ml_repo >> binary_masks_group




