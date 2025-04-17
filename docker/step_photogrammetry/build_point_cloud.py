import json
import os
import sys
from pathlib import Path
import Metashape

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

def build_point_cloud():
    from config.data_source import data_sources

    """
    Build Point Cloud process
    """

    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, '..', 'init_set.json')

    with open(params_path, 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    Metashape.app.cpu_enable = init_out.get('cpu_enable')
    Metashape.app.gpu_mask = init_out.get('gpu_mask')
    output_folder = init_out.get('project_output')

    config_path = os.path.join(script_dir, '..', 'inputs_photogrammetry', 'params.json')
    with open(config_path, 'r') as f:
        cfg = json.load(f)

    task_config = cfg.get("buildPointCloud", {})
    source_data_str = task_config.get('source_data', "Metashape.DataSource.DepthMapsData")
    source_data = data_sources.get(source_data_str, Metashape.DataSource.DepthMapsData)
    point_colors = task_config.get('point_colors', True)
    point_confidence = task_config.get('point_confidence', True)
    keep_depth = task_config.get('keep_depth', True)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=False)
    chunk = doc.chunks[0]

    chunk.buildPointCloud(source_data=source_data,
                          point_colors= point_colors,
                          point_confidence= point_confidence,
                          keep_depth= keep_depth,
                          subdivide_task = subdivide_task)

    chunk.exportPointCloud(os.path.join(output_folder, 'point_cloud.las'))
    sys.exit(0) 

if __name__ == "__main__":
    build_point_cloud()