from airflow.models import TaskInstance

def match_and_align(**kwargs):
    import Metashape
    import logging
    
    """
    Image matching and alignment
    """

    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    Metashape.app.cpu_enable = ti.xcom_pull(task_ids='data_initialise', key='cpu_enable')
    Metashape.app.gpu_mask = ti.xcom_pull(task_ids='data_initialise', key='gpu_mask')

    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("matchPhotos", {})
    downscale = task_config.get('downscale', 2)
    downscale_3d = task_config.get('downscale_3d', 4)
    keypoint_limit = task_config.get('keypoint_limit', 40000)
    tiepoint_limit = task_config.get('tiepoint_limit', 10000)
    generic_preselection = task_config.get('generic_preselection', True)
    reference_preselection = task_config.get('reference_preselection', False)
    filter_stationary_points = task_config.get('filter_stationary_points', True)
    keep_keypoints = task_config.get('keep_keypoints', True)
    guided_matching = task_config.get('guided_matching', False)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.matchPhotos(downscale=downscale, 
                      downscale_3d=downscale_3d, 
                      generic_preselection= generic_preselection, 
                      keypoint_limit=keypoint_limit, 
                      tiepoint_limit=tiepoint_limit,
                      reference_preselection = reference_preselection,
                      filter_stationary_points = filter_stationary_points,
                      keep_keypoints = keep_keypoints,
                      guided_matching = guided_matching,
                      subdivide_task = subdivide_task)
    
    task_config = dagrun_conf.get("alignCameras", {})
    adaptive_fitting = task_config.get('adaptive_fitting', False)
    reset_alignment = task_config.get('reset_alignment', True)
    subdivide_task = task_config.get('subdivide_task', True)

    chunk.alignCameras(adaptive_fitting = adaptive_fitting,
                       reset_alignment = reset_alignment,
                       subdivide_task= subdivide_task)
    
    doc.save(version="match_and_align")
    logging.info(f"Matched & Align photos.")