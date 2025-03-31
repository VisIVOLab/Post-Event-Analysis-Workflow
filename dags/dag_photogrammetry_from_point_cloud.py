from airflow import DAG
import os, sys
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))


#TODO: verificare che la GPU è abilitata sempre come la CPU a ogni singolo task
# eventualmente si setta su xcom o 

def import_dag_configuration(**kwargs):
    """
    Save the project path into the XCom space
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

def new_project(**kwargs):
    import Metashape
    import logging

    """Apre o crea un progetto Metashape."""
    # Checking compatibility
    logging.info("Checking compatibility")
    compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version))
    
    #TODO: test cpu/gpu

    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

    doc = Metashape.Document()
    doc.save(path=project_path, version="new project")
    logging.info("🚀 progetto creato!")

def import_photos(**kwargs):
    import Metashape
    import logging, os

    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    image_folder = ti.xcom_pull(task_ids="data_initialise", key='image_path')
    try:
        photos = [entry.path for entry in os.scandir(image_folder) if entry.is_file() and entry.name.lower().endswith(('.jpg', '.jpeg', '.tif', '.tiff'))]
    except Exception as e:
        print(f"Error import photos: {e}")
    
    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.addChunk()  # Aggiunge un nuovo chunk al progetto
    chunk.addPhotos(photos)
    doc.save(version="import_photos")

    logging.info(f"🚀 {len(chunk.cameras)} images loaded.")

def match_and_align(**kwargs):
    import Metashape
    import logging
    
    """Matching e allineamento delle immagini"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

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
    logging.info(f"🚀 Matched photos.")

def build_depth_maps(**kwargs):
    import Metashape
    from config.filter_modes import filter_modes

    """Costruzione Depth Maps"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

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

def build_point_cloud(**kwargs):
    import Metashape
    from config.data_source import data_sources

    """Costruzione Point Cloud"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

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
    doc.save(version="build_point_cloud")

def build_model(**kwargs):
    import Metashape
    import logging
    from config.surface_type import surface_types
    from config.interpolation import interpolations
    from config.face_count import face_counts
    from config.data_source import data_sources

    """Costruzione Modello 3D"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("buildModel", {})
    surface_type_str = task_config.get('surface_type', "Metashape.SurfaceType.Arbitrary")
    surface_type = surface_types.get(surface_type_str, Metashape.SurfaceType.Arbitrary)
    interpolation_str = task_config.get('interpolation', "Metashape.Interpolation.EnabledInterpolation")
    interpolation = interpolations.get(interpolation_str, Metashape.Interpolation.EnabledInterpolation)
    face_count_str = task_config.get('face_count', "Metashape.FaceCount.MediumFaceCount")
    face_count = face_counts.get(face_count_str, Metashape.FaceCount.MediumFaceCount)
    source_data_str = task_config.get('source_data', "Metashape.DataSource.PointCloudData")
    source_data = data_sources.get(source_data_str, Metashape.DataSource.PointCloudData)
    vertex_colors = task_config.get('vertex_colors', True)
    vertex_confidence = task_config.get('vertex_confidence', True)
    keep_depth = task_config.get('keep_depth', True)
    split_in_blocks = task_config.get('split_in_blocks', False)
    blocks_size = task_config.get('blocks_size', 250)
    build_texture = task_config.get('build_texture', True)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildModel(surface_type=surface_type, 
                     interpolation=interpolation, 
                     face_count=face_count, 
                     source_data=source_data,
                     vertex_colors=vertex_colors,
                     vertex_confidence=vertex_confidence,
                     keep_depth = keep_depth,
                     split_in_blocks = split_in_blocks,
                     blocks_size = blocks_size,
                     build_texture= build_texture,
                     subdivide_task = subdivide_task)
    chunk.exportModel(os.path.join(output_folder, 'model.obj'))
    logging.info(f"🚀 Export model.")
    #doc.save(version="build_model")

def build_tiled(**kwargs):
    import Metashape
    from config.data_source import data_sources

    """Costruzione Modello tiled"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    dagrun_conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    task_config = dagrun_conf.get("buildTiledModel", {})
    pixel_size = task_config.get('pixel_size', 0)
    tile_size = task_config.get('tile_size', 256)
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

    chunk.buildTiledModel(pixel_size = pixel_size,
                          tile_size = tile_size,
                          source_data=source_data,
                          face_count = face_count,
                          ghosting_filter = ghosting_filter,
                          transfer_texture = transfer_texture,
                          keep_depth = keep_depth,
                          subdivide_task = subdivide_task)
    print("modello tiled")
    chunk.exportTiledModel(path=os.path.join(output_folder, 'tile.zip'), format=Metashape.TiledModelFormat.TiledModelFormatZIP)
    print("modello tiled esportato")
    #doc.save(version="build_tiled")

def export_cloud(**kwargs):
    import Metashape

    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=True)
    chunk = doc.chunks[0]

    chunk.exportPointCloud(os.path.join(output_folder, 'point_cloud.las'))
    print("Esportazione completata!")

# Definizione degli argomenti di default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': False, # Il task di oggi partirà solo se quello di ieri è stato completato con successo.
    'start_date': datetime(2025, 3, 21),
    'retries': 3,
}

dag = DAG(
    dag_id='dag_photogrammetry_point_cloud_dagrun_v10',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False, # by default è su True, eseguirà lo script  in base alla schedule interval da quel giorno a oggi (mensilmente/giornalmente ecc)
    # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
    tags= ['dagrun', 'point cloud', 'no save']
)

# Definizione dei task
task_data_initialise = PythonOperator(
    task_id="data_initialise",
    python_callable=import_dag_configuration,
    provide_context=True
)

task_new_project = PythonOperator(
    task_id='new_project',
    python_callable=new_project,
    provide_context=True,
    dag=dag
)

task_import_photos = PythonOperator(
    task_id='import_photos',
    python_callable=import_photos,
    provide_context=True,
    dag=dag
)

task_match_and_align = PythonOperator(
    task_id='match_and_align',
    python_callable=match_and_align,
    provide_context=True,
    dag=dag
)

task_build_depth_maps = PythonOperator(
    task_id='build_depth_maps',
    python_callable=build_depth_maps,
    provide_context=True,
    dag=dag
)

task_build_point_cloud = PythonOperator(
    task_id='build_point_cloud',
    python_callable=build_point_cloud,
    provide_context=True,
    dag=dag
)

task_build_model = PythonOperator(
    task_id='build_model',
    python_callable=build_model,
    provide_context=True,
    dag=dag
)

task_build_tiled = PythonOperator(
    task_id='build_tiled',
    python_callable=build_tiled,
    provide_context=True,
    dag=dag
)

task_export_cloud = PythonOperator(
    task_id='export_cloud',
    python_callable=export_cloud,
    provide_context=True,
    dag=dag
)

# Definizione delle dipendenze
task_data_initialise >> task_new_project >> task_import_photos >> task_match_and_align >> task_build_depth_maps >> task_build_point_cloud >> [task_export_cloud, task_build_tiled, task_build_model]
