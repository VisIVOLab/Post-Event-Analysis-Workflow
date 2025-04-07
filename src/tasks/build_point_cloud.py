from airflow.models import TaskInstance

def build_point_cloud(**kwargs):
    import Metashape
    import os, logging
    from config.data_source import data_sources

    """Build Point Cloud process"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    Metashape.app.cpu_enable = ti.xcom_pull(task_ids='data_initialise', key='cpu_enable')
    Metashape.app.gpu_mask = ti.xcom_pull(task_ids='data_initialise', key='gpu_mask')
    dag = kwargs.get('dag')
    if dag and 'depth map' in dag.tags:
        output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("buildPointCloud", {})
    source_data_str = task_config.get('source_data', "Metashape.DataSource.DepthMapsData")
    source_data = data_sources.get(source_data_str, Metashape.DataSource.DepthMapsData)
    point_colors = task_config.get('point_colors', True)
    point_confidence = task_config.get('point_confidence', True)
    keep_depth = task_config.get('keep_depth', True)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildPointCloud(source_data=source_data,
                          point_colors= point_colors,
                          point_confidence= point_confidence,
                          keep_depth= keep_depth,
                          subdivide_task = subdivide_task)
    if dag and 'depth map' in dag.tags:
        chunk.exportPointCloud(os.path.join(output_folder, 'point_cloud.las'))
        logging.info(f"Export point cloud.")
    else:   # point cloud
        doc.save(version="build_point_cloud")
        logging.info(f"Build point cloud.")