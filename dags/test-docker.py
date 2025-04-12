import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

class TemplatedDockerOperator(DockerOperator):
    template_fields = DockerOperator.template_fields + ("mounts",)



import os
import sys
from pathlib import Path

# airflow dags trigger test-docker --conf "$(cat dagrun-ml.cfg)"


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))


class DynamicMountDockerOperator(DockerOperator):
    # Rendi 'source_path' un campo templabile
    template_fields = DockerOperator.template_fields + ("input_path","output_path",)

    def __init__(
        self,
        input_path: str,  # path assoluto da dag_run.conf
        output_path: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_path = output_path

    def execute(self, context):
        # Ora self.source_path è già stato espanso da Jinja
        self.mounts = [
            Mount(source=self.input_path, target='/ml/input', type="bind", read_only=True),
            Mount(source=self.output_path, target='/ml/output', type="bind", read_only=False),
        ]
        return super().execute(context)

# Default
default_args = {
    'owner': 'Visivo',
    'project_root_folder': project_root_folder,
    'data_folder': project_root_folder / '/data',
    'ml_folder': '/ml',
    'repo_folder': '/ml/repo',
    'repo_url': "https://github.com/ICSC-Spoke3/HaMMon-ML-digital-twin.git",
    'max_label':12,
    "downscale_size": 713
}


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

dag = DAG(
    dag_id='test-docker',
    default_args=default_args,
    schedule_interval=None, # Manual start
    tags = ['ml']
)




# # check and resize input images
# resize_images = BashOperator(
#     task_id='resize_images',
#     bash_command=f"""
#         TMP_DIR={{{{ dag_run.conf['output_folder'] }}}}/tmp_{{{{ dag_run.run_id }}}}
#         RESIZED_DIR=$TMP_DIR/resized_inputs
#         mkdir -p $RESIZED_DIR

#         python {default_args['repo_folder']}/utils/preprocess_resize.py \
#             {{{{ dag_run.conf['input_folder'] }}}} \
#             $RESIZED_DIR \
#             {default_args['downscale_size']}
#     """,
#     do_xcom_push=True,
#     dag=dag
# )

resize_images = DynamicMountDockerOperator(
    task_id='resize_images',
    image='pil-processing:latest',
    container_name='pil-processing-container',
    api_version='auto',
    auto_remove="force",
    command=f"""
        bash -c '
        TMP_DIR=/ml/output/tmp_{{{{ dag_run.run_id }}}}
        echo "TMP_DIR: $TMP_DIR"
        RESIZED_DIR=$TMP_DIR/resized_inputs
        echo "RESIZED_DIR: $RESIZED_DIR"
        mkdir -p $RESIZED_DIR

        echo " ls input"
        ls -la /ml/input

        echo  " output directory $RESIZED_DIR"

        python {default_args['repo_folder']}/utils/preprocess_resize.py \
            /ml/input \
            $RESIZED_DIR \
            {default_args['downscale_size']}
        echo "output folder: $RESIZED_DIR"
        ls -la $RESIZED_DIR '
    """,
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    mount_tmp_dir=False,
    input_path="{{dag_run.conf['input_folder']}}",
    output_path="{{dag_run.conf['output_folder']}}",
    user=f"{os.getuid()}",

    # mounts=[
    #     Mount(source="{{dag_run.conf['input_folder']}}", target="/ml/input", type="bind", read_only=True),
    #     Mount(source="{{dag_run.conf['output_folder']}}", target="/ml/output", type="bind"),
    # ],
    dag=dag,
)




# Task chaining

(
    resize_images
)




