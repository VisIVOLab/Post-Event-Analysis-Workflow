import os
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

from src.dynamic_mount_docker_operator import DynamicMountDockerOperator


# airflow dags trigger ml-docker --conf "$(cat dagrun-ml.cfg)"

# Default
default_args = {
    'owner': 'Visivo',
    'project_root_folder':str(project_root_folder),
    'data_folder': str(project_root_folder / 'data'),
    'ml_folder': str(project_root_folder / 'ml/'),
    'repo_url': "https://github.com/ICSC-Spoke3/HaMMon-ML-digital-twin.git",
    'target_src_path': "/ml/src",
    'target_input_path': "/ml/input",
    'target_output_path': "/ml/output",
    'max_label':12,
    "downscale_size": 713
}

class MlDynamicDockerOperator(DynamicMountDockerOperator):
    def __init__(self, 
                 task_id: str, 
                 command: str,
                 image: str,
                 container_name: str, 
                 **kwargs):
        super().__init__(
            task_id=task_id,
            image=image,
            container_name=container_name,
            command=command,
            api_version='auto',
            auto_remove="force",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mount_tmp_dir=False,
            input_path = '/home/mauro/projects/AirflowDemo/data/test-input-small',
            # input_path="{{dag_run.conf['input_folder']}}",
            target_input_path=default_args['target_input_path'],
            output_path='/home/mauro/projects/AirflowDemo/data/ml-output',
            #output_path="{{dag_run.conf['output_folder']}}",
            target_output_path=default_args['target_output_path'],
            src_path=default_args['ml_folder'],
            target_src_path=default_args['target_src_path'],
            user=f"{os.getuid()}",
            **kwargs
        )




dag = DAG(
    dag_id='ml-docker',
    default_args=default_args,
    schedule_interval=None, # Manual start
    tags = ['ml']
)


# labels = [
#     ("0", "background"),
# #    ("1", "building-flooded"),
#     ("2", "building-not-flooded"),
# #    ("3", "road-flooded"),
#     ("4", "road-not-flooded"),
#     ("5", "water"),
#     ("6", "tree"),
#     ("7", "vehicle"),
# #    ("8", "pool"),
#     ("9", "grass"),
# ]

labels = [
    ("0", "background"),
    ("1", "water"),
    ("2", "building_no_damage"),
    ("3", "building_minor_damage"),
    ("4", "building_major_damage"),
    ("5", "building_total_destruction"),
    ("6", "vehicle"),
    ("7", "road-clear"),
    ("8", "road-blocked"),
    ("9", "tree"),
    ("10", "pool"),
]

# into the dagrun.cfg
#   "input_folder":  
#   "output_folder"  


git_task = MlDynamicDockerOperator(
    task_id="clone_or_pull_repo",
    image="bitnami/git",  # oppure un'immagine custom se ti serve qualcosa di diverso
    container_name="git_clone_container",
    command=f"bash -c '\
        cd {default_args['target_src_path']}/repo 2>/dev/null && git pull || \
        git clone {default_args['repo_url']} {default_args['target_src_path']}/repo \
            ' ",
    dag=dag
)
        # && TMP_DIR={default_args["target_output_path"]}/tmp_{{{{ dag_run.run_id }}}} \
        # && mkdir -p $TMP_DIR \

# check and resize input images
resize_images = MlDynamicDockerOperator(
    task_id='resize_images',
    image="pil-processing:latest",
    container_name="resize_container",
    command=f""" bash -c '
        set -e
        TMP_DIR={default_args['target_output_path']}/tmp_{{{{ dag_run.run_id }}}} 
        RESIZED_DIR=$TMP_DIR/resized_inputs 
        
        mkdir -p $RESIZED_DIR 
        cd $TMP_DIR 
        
        python {default_args['target_src_path']}/repo/utils/preprocess_resize.py \
        {default_args['target_input_path']} \
        $RESIZED_DIR \
        {default_args['downscale_size']} \
        ' """,
    dag=dag
)

# batch_inference = MlDynamicDockerOperator(
#     task_id='batch_inference',
#     image="ml-inference:latest",
#     container_name="ml_inference_container",
#     command=f""" bash -c '
#         set -e
#         TMP_DIR={default_args['target_output_path']}/tmp_{{{{ dag_run.run_id }}}} 
#         RESIZED_DIR=$TMP_DIR/resized_inputs 
#         ML_OUTPUTS=$TMP_DIR/ml_outputs 
        
#         mkdir -p $ML_OUTPUTS 
#         cd $TMP_DIR 
        
#         python {default_args['target_src_path']}/repo/utils/batch_inference.py \
#             $RESIZED_DIR \
#             $ML_OUTPUTS \
#         ' """,
#     dag=dag
# )


# DUMMY BATCH INFERENCE
dummy_batch_inference = BashOperator(
    task_id='dummy_batch_inference',
    bash_command=f"""
        echo "TMP_DIR will be: {default_args['data_folder']}/ml-output/tmp_{{{{ dag_run.run_id }}}}" && \
        TMP_DIR={default_args['data_folder']}/ml-output/tmp_{{{{ dag_run.run_id }}}} && \
        mkdir -p $TMP_DIR/ml_outputs && \
        cp -r {default_args['data_folder']}/ml-dummy-output/* $TMP_DIR/ml_outputs/
    """,
    dag=dag
)

upscale_masks = MlDynamicDockerOperator(
    task_id='upscale_masks',
    image="pil-processing:latest",
    container_name="upscale_container",
    command=f""" bash -c '
        set -e
        TMP_DIR={default_args['target_output_path']}/tmp_{{{{ dag_run.run_id }}}}
        cat $TMP_DIR/resize_info.json
        ' """,
    dag=dag
)


# upscale_masks = BashOperator(
#     task_id='upscale_masks',
#     bash_command=f"""
#         TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
#         UPSCALE_SIZE="{{{{ ti.xcom_pull(task_ids='resize_images') }}}}"

#         python {default_args['repo_folder']}/utils/masks_resize.py \
#             $TMP_DIR/ml_outputs/ \
#             $TMP_DIR/ml_outputs_upscaled/ \
#             $UPSCALE_SIZE
#     """,
#     dag=dag
# )


# task chaining
(
    git_task
    >> resize_images
    >> dummy_batch_inference
    >> upscale_masks
)






# validate_masks = BashOperator(
#     task_id='validate_masks',
#     bash_command=f"""
#         TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}

#         python {default_args['repo_folder']}/utils/masks_validate.py \
#             $TMP_DIR/ml_outputs_upscaled \
#             0 {default_args['max_label']}
#     """,
#     dag=dag
# )

# prepare_binary_dir = BashOperator(
#     task_id="prepare_binary_dir",
#     bash_command=f"""
#         RUN_ID="{{{{ dag_run.run_id }}}}"
#         OUTPUT_FOLDER={{{{ dag_run.conf['output_folder'] }}}}
#         BINARY_DIR="$OUTPUT_FOLDER/binary"
#         FINAL_DIR="$BINARY_DIR"

#         if [ -d "$BINARY_DIR" ]; then
#             echo "BINARY_DIR already exists. Creating a new folder with run_id."
#             FINAL_DIR="${{BINARY_DIR}}_${{RUN_ID}}"
#             mkdir -p "$FINAL_DIR"
#         else
#             mkdir -p "$FINAL_DIR"
#         fi
#     """,
#     dag=dag
# )

# with dag: 
#     with TaskGroup("binary_masks_group", tooltip="extract binary masks") as binary_masks_group:
#         binary_mask_tasks = []
#         for i, (num, label) in enumerate(labels):
#             task = BashOperator(
#                 task_id=f"binary_mask_{i}",
#                 bash_command=f"""
#                     RUN_ID="{{{{ dag_run.run_id }}}}"
#                     TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_$RUN_ID
#                     OUTPUT_FOLDER={{{{ dag_run.conf['output_folder'] }}}}
#                     BINARY_DIR="$OUTPUT_FOLDER/binary"
#                     BINARY_RUN_ID="$OUTPUT_FOLDER/binary_$RUN_ID"

#                     # if the run_id folder exists, use it. Otherwise, use the default binary folder
#                     if [ -d "$BINARY_RUN_ID" ]; then
#                         BINARY_DIR="$BINARY_RUN_ID"
#                     else
#                         BINARY_DIR="$BINARY_DIR"
#                     fi

#                     mkdir -p "$BINARY_DIR/{label}"

#                     python {default_args['repo_folder']}/utils/masks_binary.py \
#                         $TMP_DIR/ml_outputs_upscaled  \
#                         {num} \
#                         "$BINARY_DIR/{label}"
#                 """,
#                 dag=dag
#             )
#             binary_mask_tasks.append(task)

# for t1, t2 in zip(binary_mask_tasks[:-1], binary_mask_tasks[1:]):
#     t1 >> t2

# cleanup = BashOperator(
#     task_id='clean_up',
#     bash_command=f"""
#         TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
        
#         if [[ "$TMP_DIR" == *"/tmp_"* ]]; then
#             echo "Cleaning up $TMP_DIR"
#             rm -rf "$TMP_DIR"
#         else
#             echo "Not deleting $TMP_DIR - safety check failed!"
#             exit 1
#         fi
#     """,
#     dag=dag
# )


# rename_files = BashOperator(
#     task_id="rename_files",
#     bash_command=f"""
#         # Set the output folder from dag_run configuration.
#         OUTPUT_FOLDER={{{{ dag_run.conf['output_folder'] }}}}
#         BINARY_DIR="$OUTPUT_FOLDER/binary"
        
#         # If a run-specific binary folder exists, use it.
#         if [ -d "$OUTPUT_FOLDER/binary_{{{{ dag_run.run_id }}}}" ]; then
#             BINARY_DIR="$OUTPUT_FOLDER/binary_{{{{ dag_run.run_id }}}}"
#         fi

#         echo "Using BINARY_DIR: $BINARY_DIR"
#         bash {default_args['project_root_folder']}/src/scripts/rename_binary_masks.sh "$BINARY_DIR"
#     """,
#     dag=dag
# )



# # Task chaining

# (
#     setup_ml_repo
#     >> install_requirements
#     >> resize_images
#     >> batch_inference
#     >> upscale_masks
#     >> validate_masks
#     >> prepare_binary_dir
#     >> binary_masks_group
#     >> rename_files
#     >> cleanup
# )




