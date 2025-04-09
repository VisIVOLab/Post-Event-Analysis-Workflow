import json
import os
import sys
#import Metashape

def new_project():
    """
    New Metashape project
    """
    with open(sys.argv[1], 'r') as f:
        init_out = json.load(f)

    # Check compatibility
    """ compatible_major_version = "2.2"
    found_major_version = ".".join(Metashape.app.version.split('.')[:2])
    if found_major_version != compatible_major_version:
        raise Exception("Incompatible Metashape version: {} != {}".format(found_major_version, compatible_major_version)) """
    
    project_path = init_out.get('project_path')
    print("new", project_path)
    #doc = Metashape.Document()
    #doc.save(path=project_path, version="new project")

if __name__ == "__main__":
    new_project()