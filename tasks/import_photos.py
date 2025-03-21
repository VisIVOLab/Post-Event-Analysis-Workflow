import Metashape
import os
import logging

def import_photos(image_folder, output_folder):
    print("Import Photos")
    photos = [entry.path for entry in os.scandir(image_folder) if entry.is_file() and entry.name.lower().endswith(('.jpg', '.jpeg', '.tif', '.tiff'))]
    
    doc = Metashape.app.document
    project_path = os.path.join(output_folder, "project.psx")
    doc.open(project_path)
    chunk = doc.addChunk()  # Aggiunge un nuovo chunk al progetto
    chunk.addPhotos(photos)
    doc.save()
    
    print(f"{len(chunk.cameras)} images loaded.")
    logging.info(f"🚀 {len(chunk.cameras)} images loaded.")
