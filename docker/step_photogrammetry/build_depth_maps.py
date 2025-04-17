import json
import os
import sys
from pathlib import Path
import Metashape

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

def build_depth_maps():
    from config.filter_modes import filter_modes

    """
    Build Depth Maps process
    """

    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, '..', 'init_set.json')

    with open(params_path, 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    Metashape.app.cpu_enable = init_out.get('cpu_enable')
    Metashape.app.gpu_mask = init_out.get('gpu_mask')

    config_path = os.path.join(script_dir, '..', 'inputs_photogrammetry', 'params.json')
    with open(config_path, 'r') as f:
        cfg = json.load(f)

    task_config = cfg.get("buildDepthMaps", {})
    downscale = task_config.get('downscale', 4)
    filter_mode_str = task_config.get('filter_mode', "Metashape.FilterMode.MildFiltering")
    filter_mode = filter_modes.get(filter_mode_str, Metashape.FilterMode.MildFiltering)
    reuse_depth = task_config.get('reuse_depth', False)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildDepthMaps(downscale=downscale, 
                         filter_mode=filter_mode,
                         reuse_depth = reuse_depth,
                         subdivide_task = subdivide_task)
    doc.save(version="build_depth_maps")
    print(f"Build depth maps.")

    sys.exit(0)

if __name__ == "__main__":
    build_depth_maps()