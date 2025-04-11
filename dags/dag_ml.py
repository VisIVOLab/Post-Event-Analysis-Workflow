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
    dag_id='ml',
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

# check and resize input images
resize_images = BashOperator(
    task_id='resize_images',
    bash_command=f"""
        TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
        RESIZED_DIR=$TMP_DIR/resized_inputs
        mkdir -p $RESIZED_DIR

        python {default_args['repo_folder']}/utils/preprocess_resize.py \
            {{{{ dag_run.conf['input_folder'] }}}} \
            $RESIZED_DIR \
            {default_args['downscale_size']}
    """,
    do_xcom_push=True,
    dag=dag
)

batch_inference = BashOperator(
    task_id='batch_inference',
    bash_command=f"""
        TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}

        python {default_args['repo_folder']}/utils/batch_inference.py \
            $TMP_DIR/resized_inputs \
            $TMP_DIR/ml_outputs \
    """,
    dag=dag
)

# # DUMMY BATCH INFERENCE
# batch_inference = BashOperator(
#     task_id='batch_inference',
#     bash_command=f"""
#         TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
#         mkdir -p $TMP_DIR/ml_outputs
#         cp -r {default_args['data_folder']}/ml-dummy-output/* $TMP_DIR/ml_outputs/
#     """,
#     dag=dag
# )

upscale_masks = BashOperator(
    task_id='upscale_masks',
    bash_command=f"""
        TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
        UPSCALE_SIZE="{{{{ ti.xcom_pull(task_ids='resize_images') }}}}"

        python {default_args['repo_folder']}/utils/masks_resize.py \
            $TMP_DIR/ml_outputs/ \
            $TMP_DIR/ml_outputs_upscaled/ \
            $UPSCALE_SIZE
    """,
    dag=dag
)


validate_masks = BashOperator(
    task_id='validate_masks',
    bash_command=f"""
        TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}

        python {default_args['repo_folder']}/utils/masks_validate.py \
            $TMP_DIR/ml_outputs_upscaled \
            0 {default_args['max_label']}
    """,
    dag=dag
)

prepare_binary_dir = BashOperator(
    task_id="prepare_binary_dir",
    bash_command=f"""
        RUN_ID="{{{{ dag_run.run_id }}}}"
        OUTPUT_FOLDER={{{{ dag_run.conf['output_folder'] }}}}
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
                    TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_$RUN_ID
                    OUTPUT_FOLDER={{{{ dag_run.conf['output_folder'] }}}}
                    BINARY_DIR="$OUTPUT_FOLDER/binary"
                    BINARY_RUN_ID="$OUTPUT_FOLDER/binary_$RUN_ID"

                    # if the run_id folder exists, use it. Otherwise, use the default binary folder
                    if [ -d "$BINARY_RUN_ID" ]; then
                        BINARY_DIR="$BINARY_RUN_ID"
                    else
                        BINARY_DIR="$BINARY_DIR"
                    fi

                    mkdir -p "$BINARY_DIR/{label}"

                    python {default_args['repo_folder']}/utils/masks_binary.py \
                        $TMP_DIR/ml_outputs_upscaled  \
                        {num} \
                        "$BINARY_DIR/{label}"
                """,
                dag=dag
            )
            binary_mask_tasks.append(task)
            
for t1, t2 in zip(binary_mask_tasks[:-1], binary_mask_tasks[1:]):
    t1 >> t2

cleanup = BashOperator(
    task_id='clean_up',
    bash_command=f"""
        TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
        
        if [[ "$TMP_DIR" == *"/tmp_"* ]]; then
            echo "Cleaning up $TMP_DIR"
            rm -rf "$TMP_DIR"
        else
            echo "Not deleting $TMP_DIR - safety check failed!"
            exit 1
        fi
    """,
    dag=dag
)


rename_files = BashOperator(
    task_id="rename_files",
    bash_command=f"""
        # Set the output folder from dag_run configuration.
        OUTPUT_FOLDER={{{{ dag_run.conf['output_folder'] }}}}
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
    setup_ml_repo
    >> install_requirements
    >> resize_images
    >> batch_inference
    >> upscale_masks
    >> validate_masks
    >> prepare_binary_dir
    >> binary_masks_group
    >> rename_files
    >> cleanup
)




