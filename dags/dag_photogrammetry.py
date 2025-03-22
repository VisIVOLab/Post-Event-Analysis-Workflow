from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from datetime import datetime
""" from task.align_cameras import align_cameras
from task.build_point_cloud import build_point_cloud
from task.build_mesh import build_mesh
from task.build_texture import build_texture
from task.export_results import export_results """

# Imposta la cartella di output per salvare il progetto
OUTPUT_FOLDER = "/path/"
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
    
    doc = Metashape.app.document
    # project_path = os.path.join(output_folder, "project.psx")
    doc.open(PROJECT_PATH)
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
    doc = Metashape.app.document
    # project_path = os.path.join(output_folder, "project.psx")
    doc.open(PROJECT_PATH)
    chunk = doc.chunks[0]

    chunk.matchPhotos(downscale=2, downscale_3d=4, generic_preselection= True, keypoint_limit=40000, tiepoint_limit=10000)
    chunk.alignCameras()
    doc.save(version="match_and_align")
    
    print(f"{len(chunk.cameras)} images loaded.")
    logging.info(f"🚀 Matched photos.")

def build_depth_maps():
    import Metashape

    """Costruzione Depth Maps"""
    doc = Metashape.app.document
    doc.open(PROJECT_PATH)  # Carica il progetto
    chunk = doc.chunks[0]

    chunk.buildDepthMaps(downscale=4, filter_mode=Metashape.MildFiltering)
    doc.save(version="build_depth_maps")

def build_point_cloud():
    import Metashape

    """Costruzione Point Cloud"""
    doc = Metashape.app.document
    doc.open(PROJECT_PATH)  # Carica il progetto
    chunk = doc.chunks[0]

    chunk.buildPointCloud(source_data=Metashape.DataSource.DepthMapsData)    
    doc.save(version="build_point_cloud")

def build_model():
    import Metashape

    """Costruzione Modello 3D"""
    doc = Metashape.app.document
    doc.open(PROJECT_PATH)
    chunk = doc.chunks[0]

    chunk.buildModel(surface_type=Metashape.SurfaceType.Arbitrary, interpolation=Metashape.Interpolation.EnabledInterpolation, face_count=Metashape.FaceCount.MediumFaceCount, source_data=Metashape.DataSource.PointCloudData)
    doc.save(version="build_model")

def build_tiled():
    import Metashape

    """Costruzione Modello tiled"""
    doc = Metashape.app.document
    doc.open(PROJECT_PATH)
    chunk = doc.chunks[0]

    chunk.buildTiledModel(source_data=Metashape.DataSource.PointCloudData)
    doc.save(version="build_tiled")

def export_cloud():
    import Metashape

    doc = Metashape.app.document
    doc.open(PROJECT_PATH)
    chunk = doc.chunks[0]

    chunk.exportPointCloud(os.path.join(OUTPUT_FOLDER, 'point_cloud.las'))
    print("Esportazione completata!")

def export_model():
    import Metashape
    
    doc = Metashape.app.document
    doc.open(PROJECT_PATH)
    chunk = doc.chunks[0]

    chunk.exportModel(os.path.join(OUTPUT_FOLDER, 'model.obj'))
    print("Esportazione completata!")

def export_tiled():
    import Metashape
    
    doc = Metashape.app.document
    doc.open(PROJECT_PATH)
    chunk = doc.chunks[0]
    chunk.exportTiledModel(os.path.join(OUTPUT_FOLDER, 'tile.zip'))
    print("Esportazione completata!")


# Definizione degli argomenti di default
default_args = {
    'owner': 'Visivo',
    'depends_on_past': True, # Il task di oggi partirà solo se quello di ieri è stato completato con successo.
    'start_date': datetime(2025, 3, 21),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_photogrammetry_v4',
    default_args=default_args,
    schedule_interval=None,  # Avvio manuale per ora
    catchup=False # Airflow ignorerà le date mancanti ed eseguirà solo la prossima esecuzione pianificata
)

# Definizione dei task

task_new_project = PythonOperator(
    task_id='new_project',
    python_callable=new_project,
    op_kwargs={'output_folder': '/home/leonardo/AirflowDemo/metashape_output'},
    dag=dag
)

task_import_photos = PythonOperator(
    task_id='import_photos',
    python_callable=import_photos,
    op_kwargs={'image_folder': '/home/leonardo/AirflowDemo/metashape_input', 'output_folder': '/home/leonardo/AirflowDemo/metashape_output'},
    dag=dag
)

""" t2 = PythonOperator(
    task_id='align_cameras',
    python_callable=align_cameras,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t3 = PythonOperator(
    task_id='build_point_cloud',
    python_callable=build_point_cloud,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t4 = PythonOperator(
    task_id='build_mesh',
    python_callable=build_mesh,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t5 = PythonOperator(
    task_id='build_texture',
    python_callable=build_texture,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
)

t6 = PythonOperator(
    task_id='export_results',
    python_callable=export_results,
    op_kwargs={'output_folder': '/path/to/output'},
    dag=dag
) """

# Definizione delle dipendenze
task_new_project #>> task_import_photos #>> t3 >> t4 >> t5 >> t6
