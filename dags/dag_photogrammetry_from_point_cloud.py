from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException


def import_dag_configuration(**kwargs):
    """
    Save the project path into the XCom space
    """
    conf = kwargs.get('dag_run').conf or {}  # Recupera parametri passati
    ti: TaskInstance = kwargs['ti']
    
    # refine project.psx path
    conf = kwargs['dag_run'].conf if 'dag_run' in kwargs else {}
    project_path = conf.get('project_path', '.')
    ti.xcom_push(key='output_path', value=project_path)
    project_path = os.path.join(project_path, "project.psx")
    image_path = conf.get('image_path', None)

    # save on xcom
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

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]
    # condizione = kwargs.get('dag_run').conf.get('interrompi', False)

    chunk.matchPhotos(downscale=2, downscale_3d=4, generic_preselection= True, keypoint_limit=40000, tiepoint_limit=10000)
    chunk.alignCameras()
    doc.save(version="match_and_align")
    logging.info(f"🚀 Matched photos.")

def build_depth_maps(**kwargs):
    import Metashape

    """Costruzione Depth Maps"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildDepthMaps(downscale=4, filter_mode=Metashape.MildFiltering)
    doc.save(version="build_depth_maps")

def build_point_cloud(**kwargs):
    import Metashape

    """Costruzione Point Cloud"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildPointCloud(source_data=Metashape.DataSource.DepthMapsData)    
    doc.save(version="build_point_cloud")

def build_model(**kwargs):
    import Metashape
    import logging

    """Costruzione Modello 3D"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildModel(surface_type=Metashape.SurfaceType.Arbitrary, interpolation=Metashape.Interpolation.EnabledInterpolation, face_count=Metashape.FaceCount.MediumFaceCount, source_data=Metashape.DataSource.PointCloudData)
    chunk.exportModel(os.path.join(output_folder, 'model.obj'))
    logging.info(f"🚀 Export model.")
    #doc.save(version="build_model")

def build_tiled(**kwargs):
    import Metashape

    """Costruzione Modello tiled"""
    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    output_folder = ti.xcom_pull(task_ids='data_initialise', key='output_path')

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=True)
    chunk = doc.chunks[0]

    chunk.buildTiledModel(source_data=Metashape.DataSource.PointCloudData)
    print("Creato")
    chunk.exportTiledModel(path=os.path.join(output_folder, 'tile.zip'), format=Metashape.TiledModelFormat.TiledModelFormatZIP)
    print("Exportato")
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
    'retries': 1,
}

dag = DAG(
    dag_id='dag_photogrammetry_point_cloud_dagrun_v2',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False, # by default è su True, eseguirà lo script  in base alla schedule interval da quel giorno a oggi (mensilmente/giornalmente ecc)
    # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
    tags= ['dagrun', 'point cloud', 'no save']
)

task_data_initialise = PythonOperator(
    task_id="data_initialise",
    python_callable=import_dag_configuration,
    provide_context=True
)

# Definizione dei task
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
task_data_initialise >> task_new_project >> task_import_photos >> task_match_and_align >> task_build_depth_maps >> task_build_point_cloud >> [task_build_tiled, task_export_cloud, task_build_model]
