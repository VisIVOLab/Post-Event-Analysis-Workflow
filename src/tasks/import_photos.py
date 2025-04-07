from airflow.models import TaskInstance

def import_photos(**kwargs):
    import Metashape
    import logging, os

    ti: TaskInstance = kwargs['ti']
    project_path = ti.xcom_pull(task_ids='data_initialise', key='project_path')
    image_folder = ti.xcom_pull(task_ids="data_initialise", key='image_path')
    Metashape.app.cpu_enable = ti.xcom_pull(task_ids='data_initialise', key='cpu_enable')
    Metashape.app.gpu_mask = ti.xcom_pull(task_ids='data_initialise', key='gpu_mask')
    try:
        photos = [entry.path for entry in os.scandir(image_folder) if entry.is_file() and entry.name.lower().endswith(('.jpg', '.jpeg', '.tif', '.tiff'))]
    except Exception as e:
        print(f"Error import photos: {e}")
    
    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.addChunk()
    chunk.addPhotos(photos)

    # filter photos by image quality
    chunk.analyzeImages(cameras = chunk.cameras, filter_mask= False)
    disabled_photos = 0
    for camera in chunk.cameras:
        if float(camera.meta['Image/Quality']) < 0.5:
            camera.enabled = False
            disabled_photos += 1

    doc.save(version="import_photos")
    logging.info(f"{len(chunk.cameras)} images loaded.")
    logging.info(f"{disabled_photos} images disabled.")
