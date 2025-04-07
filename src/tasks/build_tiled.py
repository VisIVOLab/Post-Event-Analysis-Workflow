from airflow.models import TaskInstance

def build_tiled(**kwargs):
    import Metashape
    import logging, os
    from config.data_source import data_sources

    """Build Tiled model process"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("buildTiledModel", {})
    pixel_size = task_config.get('pixel_size', 0)
    tile_size = task_config.get('tile_size', 256)
    dag = kwargs.get('dag')
    if dag and 'depth map' in dag.tags:
        source_data_str = task_config.get('source_data', "Metashape.DataSource.DepthMapsData")
        source_data = data_sources.get(source_data_str, Metashape.DataSource.DepthMapsData)
    else:
        source_data_str = task_config.get('source_data', "Metashape.DataSource.PointCloudData")
        source_data = data_sources.get(source_data_str, Metashape.DataSource.PointCloudData)
    face_count = task_config.get('face_count', 20000)
    ghosting_filter = task_config.get('ghosting_filter', False)
    transfer_texture = task_config.get('transfer_texture', False)
    keep_depth = task_config.get('keep_depth', True)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=True)
    chunk = doc.chunks[0]
    Metashape.app.cpu_enable = ti.xcom_pull(task_ids='data_initialise', key='cpu_enable')
    Metashape.app.gpu_mask = ti.xcom_pull(task_ids='data_initialise', key='gpu_mask')

    chunk.buildTiledModel(pixel_size = pixel_size,
                          tile_size = tile_size,
                          source_data=source_data,
                          face_count = face_count,
                          ghosting_filter = ghosting_filter,
                          transfer_texture = transfer_texture,
                          keep_depth = keep_depth,
                          subdivide_task = subdivide_task)
    chunk.exportTiledModel(path=os.path.join(output_folder, 'tile.zip'), format=Metashape.TiledModelFormat.TiledModelFormatZIP)
    logging.info(f" Export tiled model.")