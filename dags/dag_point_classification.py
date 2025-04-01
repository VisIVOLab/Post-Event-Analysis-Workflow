from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os, sys
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

# Imposta la cartella di output per salvare il progetto
OUTPUT_FOLDER = "/home/leonardo/AirflowDemo/metashape_output"
PROJECT_PATH = "/home/leonardo/AirflowDemo/progetto_ottignana/progetto_ottignana.psx"
MASK_PATH = "/home/leonardo/AirflowDemo/maschere"

def import_mask(masks_directory: str):
    import Metashape
    import os

    if not isinstance(PROJECT_PATH, str):
        raise TypeError("The project path must be a string.")
    if not PROJECT_PATH.endswith((".psx", ".psz")):
        raise ValueError("The project file must be a .psx/.psz format")
    
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    if len(doc.chunks) == 0:    
        raise ValueError("Nessun chunk presente nel documento.")
    chunk = doc.chunks[0]

    if not chunk.cameras:
        raise ValueError("Progetto senza immagini caricate.")
    if not chunk.point_cloud and not chunk.point_cloud.point_count > 0:
        raise ValueError("Progetto senza point cloud.")
    if not os.path.isdir(masks_directory):
        raise NotADirectoryError(f"Il percorso delle maschere '{masks_directory}' non è una directory valida")


    # Importa le maschere sulle camere
    for camera in chunk.cameras:
        if camera.label:
            # Costruisce il percorso del file maschera usando il nome della camera
            mask_file_name = f"{camera.label}_mask.png" # nome formato maschere
            mask_file_path = os.path.join(masks_directory, mask_file_name)
            if os.path.isfile(mask_file_path):
                try:
                    chunk.generateMasks(path=mask_file_path, masking_mode=Metashape.MaskingMode.MaskingModeFile, mask_operation=Metashape.MaskOperation.MaskOperationReplacement, tolerance=10, cameras=[camera], replace_asset = True)
                    #print(f"Maschera importata per {camera.label}")
                except Exception as e:
                    print(f"Errore nell'importazione della maschera per {camera.label}: {e}")
            else:
                print(f"Maschera non trovata per {camera.label}") # al percorso {mask_file_path}")
    
    # Inverto maschera di selezione
    for camera in chunk.cameras:
        if camera.mask is not None:
            camera.mask = camera.mask.invert()
    doc.save(version="new project")

def select_points_by_mask(folder):
    import Metashape
    doc = Metashape.Document()
    doc.open(path=PROJECT_PATH, read_only=False)
    chunk = doc.chunks[0]
    cameras = [camera for camera in chunk.cameras if camera.mask is not None]
    try:
        chunk.point_cloud.selectMaskedPoints(cameras, softness=4, only_visible=True)
        chunk.point_cloud.assignClassToSelection(target=Metashape.PointClass.Ground)
    except Exception as e:
        print(f"Errore durante la selezione dei punti: {e}")
    doc.save(version="point cloud classification")


def export_results(folder):
    # Funzione per esportare i risultati
    print(f"Esportando risultati da {folder}")
    # Codice per esportare i risultati

# Funzione per generare dinamicamente i task
def create_dynamic_tasks(dag, base_folder):
    task_imports = []
    task_process = []
    task_exports = []
    
    # Ottieni tutte le cartelle dentro la cartella base
    folders = [f.path for f in os.scandir(base_folder) if f.is_dir()]
    
    previous_task_export = None  # Variabile per tracciare l'ultimo task export

    for folder in folders:
        # Task per importare foto
        task_import = PythonOperator(
            task_id=f"import_mask_{os.path.basename(folder)}",
            python_callable=import_mask,
            op_args=[folder],
            dag=dag
        )
        
        # Task per elaborare foto
        task_process = PythonOperator(
            task_id=f"select_points_by_mask_{os.path.basename(folder)}",
            python_callable=select_points_by_mask,
            op_args=[folder],
            dag=dag
        )

        # Task per esportare i risultati
        task_export = PythonOperator(
            task_id=f"export_results_{os.path.basename(folder)}",
            python_callable=export_results,
            op_args=[folder],
            dag=dag
        )
        
        # Imposta la sequenza di esecuzione per ogni cartella (importa -> elabora -> esporta)
        task_import >> task_process >> task_export

        # Se non c'è stato nessun task precedentemente, partiamo dal primo task export
        if previous_task_export is None:
            previous_task_export = task_export
        else:
            # Se ci sono task precedenti, il task export di questa cartella dipende dal task export precedente
            previous_task_export >> task_import
            previous_task_export = task_export

    return task_imports, task_process, task_exports

# Definisci il DAG
with DAG(
    'dynamic_task_example',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='Un esempio di DAG che itera su più cartelle',
    schedule_interval=None,  # Esegui manualmente
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Cartella di base
    base_folder = "/home/leonardo/AirflowDemo/maschere"
    
    # Crea dinamicamente i task
    create_dynamic_tasks(dag, base_folder)
