from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from datetime import datetime

# Imposta la cartella di output per salvare il progetto
OUTPUT_FOLDER = "/home/leonardo/AirflowDemo/metashape_output"
PROJECT_PATH = os.path.join(OUTPUT_FOLDER, "project.psx")

def new_project():
    import Metashape
    import logging

    """Apre o crea un progetto Metashape."""
    print("Create Project")
    # Checking compatibility
    logging.info("Checking compatibility")
    compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version))

    doc = Metashape.Document()
    #project_path = os.path.join(output_folder, "project.psx")
    doc.save(path=PROJECT_PATH, version="new project")
    logging.info("🚀 progetto creato!")

def import_photos(image_folder):
    import Metashape
    import logging, os
    
    print("Import Photos")
    photos = [entry.path for entry in os.scandir(image_folder) if entry.is_file() and entry.name.lower().endswith(('.jpg', '.jpeg', '.tif', '.tiff'))]
    
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    chunk = doc.addChunk()  # Aggiunge un nuovo chunk al progetto
    chunk.addPhotos(photos)
    doc.save(version="import_photos")
    
    print(f"{len(chunk.cameras)} images loaded.")
    logging.info(f"🚀 {len(chunk.cameras)} images loaded.")

def match_and_align():
    import Metashape
    import logging
    
    print("Match and Align Cameras")
    """Matching e allineamento delle immagini"""
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    chunk = doc.chunks[0]

    chunk.matchPhotos(downscale=2, downscale_3d=4, generic_preselection= True, keypoint_limit=40000, tiepoint_limit=10000)
    chunk.alignCameras()
    doc.save(version="match_and_align")
    logging.info(f"🚀 Matched photos.")

def build_depth_maps():
    import Metashape

    """Costruzione Depth Maps"""
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildDepthMaps(downscale=4, filter_mode=Metashape.MildFiltering)
    doc.save(version="build_depth_maps")

def build_point_cloud():
    import Metashape

    """Costruzione Point Cloud"""
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildPointCloud(source_data=Metashape.DataSource.DepthMapsData)    
    doc.save(version="build_point_cloud")

def build_model():
    import Metashape

    """Costruzione Modello 3D"""
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildModel(surface_type=Metashape.SurfaceType.Arbitrary, interpolation=Metashape.Interpolation.EnabledInterpolation, face_count=Metashape.FaceCount.MediumFaceCount, source_data=Metashape.DataSource.PointCloudData)
    print("Crea modello")
    chunk.exportModel(os.path.join(OUTPUT_FOLDER, 'model.obj'))
    print("Export")
    #doc.save(version="build_model")

def build_tiled():
    import Metashape

    """Costruzione Modello tiled"""
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=True)
    chunk = doc.chunks[0]

    chunk.buildTiledModel(source_data=Metashape.DataSource.PointCloudData)
    print("Creato")
    chunk.exportTiledModel(path=os.path.join(OUTPUT_FOLDER, 'tile.zip'), format=Metashape.TiledModelFormat.TiledModelFormatZIP)
    print("Exportato")
    #doc.save(version="build_tiled")

def export_cloud():
    import Metashape

    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=True)
    chunk = doc.chunks[0]

    chunk.exportPointCloud(os.path.join(OUTPUT_FOLDER, 'point_cloud.las'))
    print("Esportazione completata!")

"""
def export_model():
    import Metashape
    
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=True)
    chunk = doc.chunks[0]

    chunk.exportModel(os.path.join(OUTPUT_FOLDER, 'model.obj'))
    print("Esportazione completata!")

def export_tiled():
    import Metashape
    
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=True)
    chunk = doc.chunks[0]
    chunk.exportTiledModel(os.path.join(OUTPUT_FOLDER, 'tile.zip'))
    print("Esportazione completata!")
    """

# Definizione degli argomenti di default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': True, # Il task di oggi partirà solo se quello di ieri è stato completato con successo.
    'start_date': datetime(2025, 3, 21),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_photogrammetry_no_save_point_cloud_v4',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False # by default è su True, eseguirà lo script  in base alla schedule interval da quel giorno a oggi (mensilmente/giornalmente ecc)
    # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
)

# Definizione dei task

task_new_project = PythonOperator(
    task_id='new_project',
    python_callable=new_project,
    dag=dag
)

task_import_photos = PythonOperator(
    task_id='import_photos',
    python_callable=import_photos,
    op_kwargs={'image_folder': '/home/leonardo/AirflowDemo/metashape_input'},
    dag=dag
)

task_match_and_align = PythonOperator(
    task_id='match_and_align',
    python_callable=match_and_align,
    dag=dag
)

task_build_depth_maps = PythonOperator(
    task_id='build_depth_maps',
    python_callable=build_depth_maps,
    dag=dag
)

task_build_point_cloud = PythonOperator(
    task_id='build_point_cloud',
    python_callable=build_point_cloud,
    dag=dag
)

task_build_model = PythonOperator(
    task_id='build_model',
    python_callable=build_model,
    dag=dag
)

task_build_tiled = PythonOperator(
    task_id='build_tiled',
    python_callable=build_tiled,
    dag=dag
)

task_export_cloud = PythonOperator(
    task_id='export_cloud',
    python_callable=export_cloud,
    dag=dag
)

"""
task_export_model = PythonOperator(
    task_id='export_model',
    python_callable=export_model,
    dag=dag
)

task_export_tiled = PythonOperator(
    task_id='export_tiled',
    python_callable=export_tiled,
    dag=dag
)
"""
# Definizione delle dipendenze
task_new_project >> task_import_photos >> task_match_and_align >> task_build_depth_maps >> task_build_point_cloud >> [task_build_tiled, task_export_cloud, task_build_model]
#task_build_model >> task_export_model
#task_build_tiled >> task_export_tiled


