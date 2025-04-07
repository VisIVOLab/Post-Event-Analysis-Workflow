from airflow.models import TaskInstance

def build_depth_maps(**kwargs):
    import Metashape
    from config.filter_modes import filter_modes

    """Build Depth Maps process"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    Metashape.app.cpu_enable = ti.xcom_pull(task_ids='data_initialise', key='cpu_enable')
    Metashape.app.gpu_mask = ti.xcom_pull(task_ids='data_initialise', key='gpu_mask')

    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("buildDepthMaps", {})
    downscale = task_config.get('downscale', 4)
    filter_mode_str = task_config.get('filter_mode', "Metashape.FilterMode.MildFiltering")
    filter_mode = filter_modes.get(filter_mode_str, Metashape.FilterMode.MildFiltering)
    reuse_depth = task_config.get('reuse_depth', False)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildDepthMaps(downscale=downscale, 
                         filter_mode=filter_mode,
                         reuse_depth = reuse_depth,
                         subdivide_task = subdivide_task)
    doc.save(version="build_depth_maps")