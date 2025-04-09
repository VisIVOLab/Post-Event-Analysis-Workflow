import json
import os
import sys
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

def build_depth_maps():
    #import Metashape
    from config.filter_modes import filter_modes

    """Build Depth Maps process"""

    with open(sys.argv[1], 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    #Metashape.app.cpu_enable = init_out.get('cpu_enable')
    #Metashape.app.gpu_mask = init_out.get('gpu_mask')

    config_path = sys.argv[1]
    with open(config_path) as f:
        cfg = json.load(f)

    task_config = cfg.get("buildDepthMaps", {})
    downscale = task_config.get('downscale', 4)
    filter_mode_str = task_config.get('filter_mode', "Metashape.FilterMode.MildFiltering")
    #filter_mode = filter_modes.get(filter_mode_str, Metashape.FilterMode.MildFiltering)
    reuse_depth = task_config.get('reuse_depth', False)
    subdivide_task = task_config.get('subdivide_task', True)

    #doc = Metashape.Document()
    #doc.open(path=project_path, read_only=False)
    #chunk = doc.chunks[0]

    """ chunk.buildDepthMaps(downscale=downscale, 
                         filter_mode=filter_mode,
                         reuse_depth = reuse_depth,
                         subdivide_task = subdivide_task) """
    #doc.save(version="build_depth_maps")
    print(f"Build depth maps.")
    with open("build_depth_maps.done", "w") as f:
        f.write("Build depth maps.")
    sys.exit(0)

if __name__ == "__main__":
    build_depth_maps()