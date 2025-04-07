
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException
import os

def set_configuration_env(**kwargs):
    import Metashape

    """
    Save project path and settings into the XCom space
    """
    ti: TaskInstance = kwargs['ti']
    
    # refine project.psx path
    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("settings", {})
    project_path = task_config.get('project_path', '.')
    # save on xcom
    ti.xcom_push(key='output_path', value=project_path)
    project_path = os.path.join(project_path, "project.psx")
    image_path = task_config.get('image_path', None)
    ti.xcom_push(key='project_path', value=project_path)
    if image_path == None:  
        raise AirflowException("Error: no image_path folder selected!")
    ti.xcom_push(key='image_path', value=image_path)
    # cpu and gpu default setting
    gpus = Metashape.app.enumGPUDevices()
    num_gpus = len(gpus)
    gpu_mask = 2**num_gpus - 1
    ti.xcom_push(key='gpu_mask', value= gpu_mask)
    ti.xcom_push(key='cpu_enable', value=False)