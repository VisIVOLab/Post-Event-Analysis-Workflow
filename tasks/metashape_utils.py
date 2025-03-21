import Metashape
import os

def new_project(output_folder):
    """Apre o crea un progetto Metashape."""
    print("New Project")
    # Checking compatibility
    compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version))

    doc = Metashape.Document()
    project_path = os.path.join(output_folder, "project.psx")
    """ if os.path.exists(project_path):
        doc.open(project_path)
        print("Old Project")
    else:
        doc.save(project_path)
        print("New Project") """
    doc.save(project_path)
    print("New Project")
