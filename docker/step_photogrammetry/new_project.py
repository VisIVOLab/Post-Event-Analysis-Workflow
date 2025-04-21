import json
import os, sys
import Metashape

def new_project():
    """
    New Metashape project
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, '..', 'init_set.json')

    with open(params_path, 'r') as f:
        init_out = json.load(f)

    # Check compatibility
    compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version))
    
    project_path = init_out.get('project_path')

    doc = Metashape.Document()
    doc.save(path=project_path, version="new project")

    Metashape.app.quit()
    sys.exit(0)


if __name__ == "__main__":
    new_project()