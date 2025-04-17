import json
import os
import sys
import Metashape
import logging

def import_photos():

    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, '..', 'init_set.json')

    with open(params_path, 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    image_folder = init_out.get('image_path')
    Metashape.app.cpu_enable = init_out.get('cpu_enable')
    Metashape.app.gpu_mask = init_out.get('gpu_mask')
    try:
        photos = [entry.path for entry in os.scandir(image_folder) if entry.is_file() and entry.name.lower().endswith(('.jpg', '.jpeg', '.tif', '.tiff'))]
    except Exception as e:
        print(f"Error import photos: {e}")
    print("import_photos", len(photos))
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
    Metashape.app.quit()
    sys.exit(0)

if __name__ == "__main__":
    import_photos()